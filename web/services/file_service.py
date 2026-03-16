"""Service layer for file management operations."""

import json
import logging
import os
import shutil
from pathlib import Path

from classification.doc_types import CODE_TO_ID, DOC_TYPES
from database.connection import DatabaseManager
from tracking.completion_checker import get_all_counterparty_statuses
from tracking.counterparty_tracker import find_or_create_counterparty, update_checklist

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.parent


def get_all_files(db: DatabaseManager, status_filter: str = "all") -> list[dict]:
    """Query all submitted files with their classification info."""
    where = ""
    params: tuple = ()
    if status_filter and status_filter != "all":
        where = "WHERE sf.status = ?"
        params = (status_filter,)

    rows = db.execute(
        f"""SELECT sf.id, sf.original_filename, sf.file_path, sf.status,
                   sf.error_message, sf.created_at,
                   dc.confidence, dc.detected_company_name, dc.model_used,
                   dt.code AS doc_type_code, dt.name_en AS doc_type_name,
                   cp.name AS counterparty_name, cp.id AS counterparty_id
            FROM submitted_files sf
            LEFT JOIN document_classifications dc
                ON dc.file_id = sf.id AND dc.is_primary = 1
            LEFT JOIN kyc_doc_types dt ON dc.doc_type_id = dt.id
            LEFT JOIN counterparties cp ON dc.counterparty_id = cp.id
            {where}
            ORDER BY sf.id DESC""",
        params,
    )
    return rows


def get_file_detail(db: DatabaseManager, file_id: int) -> dict | None:
    """Query a single file with all its classifications."""
    rows = db.execute(
        """SELECT sf.*, dc.confidence, dc.detected_company_name,
                  dc.model_used, dc.is_primary,
                  dt.code AS doc_type_code, dt.name_en AS doc_type_name,
                  cp.name AS counterparty_name
           FROM submitted_files sf
           LEFT JOIN document_classifications dc ON dc.file_id = sf.id
           LEFT JOIN kyc_doc_types dt ON dc.doc_type_id = dt.id
           LEFT JOIN counterparties cp ON dc.counterparty_id = cp.id
           WHERE sf.id = ?""",
        (file_id,),
    )
    return rows[0] if rows else None


def assign_file_classification(
    db: DatabaseManager,
    file_id: int,
    doc_type_codes: list[str],
    counterparty_name: str,
    fuzzy_threshold: int = 85,
) -> dict:
    """Manually classify a file — replicates the CLI assign logic.

    Returns dict with result info or error.
    """
    # Validate file
    row = db.execute("SELECT * FROM submitted_files WHERE id = ?", (file_id,))
    if not row:
        return {"error": f"File #{file_id} not found."}
    file_info = row[0]

    # Validate doc types
    for code in doc_type_codes:
        if code not in DOC_TYPES:
            return {"error": f"Unknown document type: '{code}'"}

    # Find or create counterparty
    if not counterparty_name:
        existing = db.execute(
            "SELECT detected_company_name FROM document_classifications WHERE file_id = ? LIMIT 1",
            (file_id,),
        )
        if existing and existing[0]["detected_company_name"]:
            counterparty_name = existing[0]["detected_company_name"]
        else:
            return {"error": "Counterparty name is required."}

    if counterparty_name.lower() == "unknown":
        return {"error": "Please enter the actual counterparty name — 'Unknown' is not valid."}

    counterparty_id = find_or_create_counterparty(db, counterparty_name, fuzzy_threshold=fuzzy_threshold)

    # Record correction for few-shot learning (before deleting old classifications)
    from classification.few_shot import record_correction
    machine_rows = db.execute(
        """SELECT dt.code FROM document_classifications dc
           LEFT JOIN kyc_doc_types dt ON dc.doc_type_id = dt.id
           WHERE dc.file_id = ? AND dc.model_used NOT IN ('manual', 'manual_web')
           ORDER BY dc.classified_at ASC LIMIT 1""",
        (file_id,),
    )
    machine_type = machine_rows[0]["code"] if machine_rows else None
    for code in doc_type_codes:
        if code != "others":
            record_correction(db, file_info["original_filename"], machine_type, code,
                              file_path=file_info["file_path"])

    # Save new classifications FIRST (keep old records until extraction succeeds)
    # Delete old extraction results since they belong to old classifications
    db.execute("DELETE FROM extraction_results WHERE file_id = ?", (file_id,))
    # Now safe to replace classifications
    db.execute("DELETE FROM document_classifications WHERE file_id = ?", (file_id,))

    current_path = Path(file_info["file_path"])
    primary_cls_id = None
    classification_ids = {}  # code -> cls_id
    for i, code in enumerate(doc_type_codes):
        doc_type_id = CODE_TO_ID[code]
        is_primary = 1 if i == 0 else 0

        cls_id = db.execute_insert(
            """INSERT OR REPLACE INTO document_classifications
               (file_id, doc_type_id, counterparty_id, detected_company_name,
                confidence, is_primary, model_used, input_tokens, output_tokens, raw_response)
               VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?)""",
            (file_id, doc_type_id, counterparty_id, counterparty_name,
             1.0, is_primary, "manual_web",
             f"Manually assigned via web: {', '.join(doc_type_codes)}"),
        )
        if i == 0:
            primary_cls_id = cls_id
        classification_ids[code] = cls_id

        update_checklist(db, counterparty_id, doc_type_id, file_id)

        # Move (first type) / copy (additional types) to classified directory
        new_path = _move_to_classified(db, current_path, counterparty_id, code, move=(i == 0))
        if i == 0 and new_path:
            current_path = new_path

    # Update file status and path
    db.execute(
        "UPDATE submitted_files SET status = 'classified', file_path = ?, error_message = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (str(current_path), file_id),
    )

    # Run extraction for ALL doc types (not just primary)
    extraction_results = {}
    for code in doc_type_codes:
        cls_id = classification_ids[code]
        ext_status = _run_extraction(db, file_id, cls_id, code, current_path)
        extraction_results[code] = ext_status
    extraction_result = extraction_results.get(doc_type_codes[0], "skipped")

    # Audit log: manual classification via web
    db.execute_insert(
        """INSERT INTO processing_log (file_id, counterparty_id, stage, action, details)
           VALUES (?, ?, 'classification', 'manual_assign_web', ?)""",
        (file_id, counterparty_id, json.dumps({
            "doc_types": doc_type_codes,
            "counterparty_name": counterparty_name,
            "extraction_results": extraction_results,
        })),
    )

    # If counterparty was already packaged, reset to trigger re-packaging
    _reset_if_completed(db, counterparty_id)

    # Check completion and auto-package
    package_result = check_and_package(db)

    return {
        "success": True,
        "file_id": file_id,
        "filename": file_info["original_filename"],
        "doc_types": doc_type_codes,
        "counterparty_id": counterparty_id,
        "counterparty_name": counterparty_name,
        "extraction": extraction_result,
        "packaged": package_result,
    }


def _run_extraction(
    db: DatabaseManager, file_id: int, classification_id: int, doc_type: str, file_path: Path,
) -> str:
    """Convert file and run extraction. Returns status string."""
    from classification.prompts import EXTRACTION_PROMPTS

    if doc_type not in EXTRACTION_PROMPTS:
        logger.info("No extraction prompt for type '%s', skipping extraction", doc_type)
        return "skipped"

    try:
        config = _load_config()
        if not config:
            return "skipped_no_config"

        # Convert file to get text/images
        from processing.file_converter import convert_file
        scan_dpi = config["processing"]["scan_dpi"]
        max_dim = config["processing"]["max_image_dimension"]
        result = convert_file(file_path, scan_dpi=scan_dpi, max_image_dim=max_dim)

        if result.error:
            logger.warning("Conversion failed for extraction: %s", result.error)
            return f"conversion_error: {result.error}"

        # Try API first, fall back to CLI
        extraction = None
        try:
            import anthropic
            client = anthropic.Anthropic()
            if not client.api_key:
                raise ValueError("Empty API key")

            from extraction.extractor import extract_document_data
            max_ext_images = config.get("classification", {}).get("max_extraction_images", 10)
            extraction = extract_document_data(
                client,
                doc_type=doc_type,
                text_content=result.text_content,
                image_paths=result.image_paths if result.image_paths else None,
                model_simple=config["claude"]["extraction_model_simple"],
                model_complex=config["claude"]["extraction_model_complex"],
                max_retries=config["claude"]["max_retries"],
                retry_base_delay=config["claude"]["retry_base_delay"],
                max_images=max_ext_images,
            )
        except Exception:
            from extraction.claude_code_extractor import extract_with_claude_code
            extraction = extract_with_claude_code(
                doc_type=doc_type,
                text_content=result.text_content,
                image_paths=result.image_paths if result.image_paths else None,
                model="sonnet",
            )

        # Save extraction result
        db.execute_insert(
            """INSERT OR REPLACE INTO extraction_results
               (file_id, classification_id, extracted_data, validation_passed,
                validation_errors, model_used, input_tokens, output_tokens, raw_response)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                file_id, classification_id,
                json.dumps(extraction.extracted_data, ensure_ascii=False),
                1 if extraction.validated else 0,
                json.dumps(extraction.validation_errors),
                extraction.model_used,
                extraction.input_tokens, extraction.output_tokens,
                extraction.raw_response,
            ),
        )

        db.execute(
            "UPDATE submitted_files SET status = 'extraction_done', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (file_id,),
        )

        # Clean up conversion artifacts (generated PNGs) from classified directory
        _cleanup_conversion_artifacts(result.image_paths, file_path)

        return "done"

    except Exception as e:
        logger.exception("Extraction failed for file #%d: %s", file_id, e)
        return f"error: {e}"


def _reset_if_completed(db: DatabaseManager, counterparty_id: int):
    """If counterparty is already 'completed' (packaged), reset to 'in_progress'
    so that check_and_package will re-package with the new file included."""
    rows = db.execute(
        "SELECT status FROM counterparties WHERE id = ?", (counterparty_id,)
    )
    if rows and rows[0]["status"] == "completed":
        db.execute(
            "UPDATE counterparties SET status = 'in_progress', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (counterparty_id,),
        )
        logger.info("Reset counterparty #%d to in_progress for re-packaging", counterparty_id)


def _cleanup_conversion_artifacts(image_paths: list[Path] | None, original_path: Path):
    """Remove temporary image files generated during conversion.

    Deletes any generated images (page renders, resized copies) that are NOT
    the original source file itself.
    """
    if not image_paths:
        return
    for img in image_paths:
        try:
            if img.exists() and img.resolve() != original_path.resolve():
                img.unlink()
                logger.debug("Cleaned up conversion artifact: %s", img)
        except Exception as e:
            logger.warning("Could not remove conversion artifact %s: %s", img, e)


def _load_config():
    """Load config.yaml — returns config dict or None."""
    import yaml
    config_path = PROJECT_ROOT / "config.yaml"
    if not config_path.exists():
        return None
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def _move_to_classified(
    db: DatabaseManager, src_path: Path, counterparty_id: int, doc_type_code: str, move: bool = True,
) -> Path | None:
    """Move or copy file to classified directory structure. Returns new path."""
    try:
        config = _load_config()
        if not config:
            return None

        cp_rows = db.execute("SELECT slug FROM counterparties WHERE id = ?", (counterparty_id,))
        if not cp_rows:
            return None

        classified_dir = PROJECT_ROOT / config["paths"]["classified"]
        doc_type_info = DOC_TYPES[doc_type_code]
        dest_dir = classified_dir / cp_rows[0]["slug"] / doc_type_info.folder_name
        dest_dir.mkdir(parents=True, exist_ok=True)

        dest = dest_dir / src_path.name
        if src_path.exists() and not dest.exists():
            if move:
                shutil.move(str(src_path), str(dest))
            else:
                shutil.copy2(str(src_path), str(dest))
        return dest
    except Exception as e:
        logger.warning("Could not move file to classified directory: %s", e)
        return None


def check_and_package(db: DatabaseManager) -> list[dict]:
    """Check for completed counterparties and package them.

    Returns list of packaged counterparty info dicts.
    """
    from notification.notifier import send_completion_notification
    from tracking.completion_checker import get_newly_completed
    from tracking.packaging import package_counterparty

    config = _load_config()
    if not config:
        return []

    newly_completed = get_newly_completed(db)
    if not newly_completed:
        return []

    classified_dir = PROJECT_ROOT / config["paths"]["classified"]
    completed_dir = PROJECT_ROOT / config["paths"]["completed"]

    smtp_config = {
        "host": config["notification"]["smtp_host"],
        "port": config["notification"]["smtp_port"],
        "user": config["notification"]["smtp_user"],
        "password": config["notification"]["smtp_password"],
        "from_address": config["notification"]["from_address"],
        "recipients": config["notification"]["compliance_team"],
    }

    results = []
    for cp_id in newly_completed:
        zip_path = package_counterparty(db, cp_id, classified_dir, completed_dir)
        if zip_path:
            send_completion_notification(db, cp_id, zip_path, smtp_config)
            cp = db.execute("SELECT name FROM counterparties WHERE id = ?", (cp_id,))
            results.append({
                "counterparty_id": cp_id,
                "name": cp[0]["name"] if cp else f"#{cp_id}",
                "zip_path": zip_path,
            })
            logger.info("Packaged counterparty #%d: %s", cp_id, zip_path)

    return results


def get_counterparty_progress(db: DatabaseManager) -> list[dict]:
    """Get counterparty progress matrix data."""
    return get_all_counterparty_statuses(db)


def get_all_counterparties(db: DatabaseManager) -> list[dict]:
    """Get all counterparties."""
    return db.execute("SELECT id, name, slug, aliases, status FROM counterparties ORDER BY name")


def _relocate_counterparty_files(source_slug: str, target_slug: str):
    """Move all files from source counterparty's classified folder into target's."""
    try:
        config = _load_config()
        if not config:
            return

        classified_dir = PROJECT_ROOT / config["paths"]["classified"]
        source_dir = classified_dir / source_slug
        target_dir = classified_dir / target_slug

        if not source_dir.exists():
            return

        for item in source_dir.rglob("*"):
            if not item.is_file():
                continue
            rel = item.relative_to(source_dir)
            dest = target_dir / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            if not dest.exists():
                shutil.move(str(item), str(dest))
            else:
                logger.warning("Skipped duplicate during merge: %s", rel)

        # Remove empty source directory tree
        shutil.rmtree(str(source_dir), ignore_errors=True)
    except Exception as e:
        logger.warning("Could not relocate files during merge: %s", e)


def _update_file_paths_after_merge(db: DatabaseManager, source_slug: str, target_slug: str):
    """Update submitted_files.file_path for files that were relocated during merge."""
    config = _load_config()
    if not config:
        return

    classified_dir = str(PROJECT_ROOT / config["paths"]["classified"])
    source_prefix = f"{classified_dir}/{source_slug}/"
    target_prefix = f"{classified_dir}/{target_slug}/"

    rows = db.execute(
        "SELECT id, file_path FROM submitted_files WHERE file_path LIKE ?",
        (f"{source_prefix}%",),
    )
    for row in rows:
        new_path = row["file_path"].replace(source_prefix, target_prefix, 1)
        db.execute(
            "UPDATE submitted_files SET file_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (new_path, row["id"]),
        )


def merge_counterparties(db: DatabaseManager, target_id: int, source_id: int) -> dict:
    """Merge source counterparty into target.

    Moves all classifications and checklist records from source to target,
    merges aliases, then deletes source.
    """
    if target_id == source_id:
        return {"error": "Cannot merge a counterparty with itself."}

    target = db.execute("SELECT * FROM counterparties WHERE id = ?", (target_id,))
    source = db.execute("SELECT * FROM counterparties WHERE id = ?", (source_id,))

    if not target:
        return {"error": f"Target counterparty #{target_id} not found."}
    if not source:
        return {"error": f"Source counterparty #{source_id} not found."}

    target = target[0]
    source = source[0]

    # Move classifications from source to target
    db.execute(
        "UPDATE document_classifications SET counterparty_id = ? WHERE counterparty_id = ?",
        (target_id, source_id),
    )

    # Update checklist: for each source checklist entry that has a file,
    # update the target's corresponding entry if it's still 'missing'
    source_checklist = db.execute(
        "SELECT * FROM counterparty_checklist WHERE counterparty_id = ? AND status != 'missing'",
        (source_id,),
    )
    for item in source_checklist:
        db.execute(
            """UPDATE counterparty_checklist
               SET status = ?, file_id = ?, updated_at = CURRENT_TIMESTAMP
               WHERE counterparty_id = ? AND doc_type_id = ? AND status = 'missing'""",
            (item["status"], item["file_id"], target_id, item["doc_type_id"]),
        )

    # Delete source checklist
    db.execute("DELETE FROM counterparty_checklist WHERE counterparty_id = ?", (source_id,))

    # Merge aliases
    target_aliases = json.loads(target["aliases"]) if target["aliases"] else []
    source_aliases = json.loads(source["aliases"]) if source["aliases"] else []
    # Add source name and aliases to target
    for name in [source["name"]] + source_aliases:
        if name not in target_aliases:
            target_aliases.append(name)

    db.execute(
        "UPDATE counterparties SET aliases = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (json.dumps(target_aliases), target_id),
    )

    # Move physical files from source slug folder to target slug folder
    _relocate_counterparty_files(source["slug"], target["slug"])

    # Update file_path in submitted_files for relocated files
    _update_file_paths_after_merge(db, source["slug"], target["slug"])

    # Delete source counterparty
    db.execute("DELETE FROM counterparties WHERE id = ?", (source_id,))

    # Audit log: counterparty merge via web
    db.execute_insert(
        """INSERT INTO processing_log (counterparty_id, stage, action, details)
           VALUES (?, 'tracking', 'merge_counterparties_web', ?)""",
        (target_id, json.dumps({
            "source_id": source_id,
            "source_name": source["name"],
            "target_id": target_id,
            "target_name": target["name"],
        })),
    )

    logger.info(
        "Merged counterparty #%d (%s) into #%d (%s)",
        source_id, source["name"], target_id, target["name"],
    )

    return {
        "success": True,
        "target_id": target_id,
        "target_name": target["name"],
        "source_id": source_id,
        "source_name": source["name"],
    }
