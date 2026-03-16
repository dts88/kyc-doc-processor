"""KYC Document Processor - CLI Entry Point & Pipeline Coordinator."""

import json
import logging
import shutil
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import click
import yaml
from dotenv import load_dotenv

# Load env before anything else
load_dotenv()

from database.connection import DatabaseManager
from database.init_db import init_database

# Configure logging (level may be overridden by config)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kyc")

# Project root
PROJECT_ROOT = Path(__file__).parent


def load_config(db: DatabaseManager | None = None) -> dict:
    """Load configuration from config.yaml and apply global settings.

    If a db instance is provided, also loads VP names for validation.
    """
    config_path = PROJECT_ROOT / "config.yaml"
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Apply logging level from config
    log_level = config.get("logging", {}).get("level", "INFO")
    logging.getLogger().setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Apply validation thresholds
    from extraction.validation import set_validation_config
    set_validation_config(config)

    # Load VP names from database if available
    if db is not None:
        from extraction.validation import load_vp_names_from_db
        load_vp_names_from_db(db)

    return config


def get_db(config: dict) -> DatabaseManager:
    """Get database manager instance."""
    db_path = PROJECT_ROOT / config["paths"]["database"]
    return DatabaseManager(str(db_path))


def resolve_path(config: dict, key: str) -> Path:
    """Resolve a relative path from config to absolute."""
    return PROJECT_ROOT / config["paths"][key]


@click.group()
def cli():
    """KYC Document Processor - Automated KYC file classification and tracking."""
    pass


@cli.command()
def init_db():
    """Initialize the database and seed document types."""
    config = load_config()
    db = get_db(config)
    init_database(db)

    # Ensure all data directories exist
    for key in ("inbox", "processing", "classified", "completed", "archive", "reports"):
        path = resolve_path(config, key)
        path.mkdir(parents=True, exist_ok=True)
        click.echo(f"  - Directory ready: {path}")

    db.close()
    click.echo("Initialization complete.")


def _recover_stuck_files(db: DatabaseManager):
    """Reset files stuck in 'processing' state (from a previous crash) back to 'pending'."""
    stuck = db.execute(
        "SELECT id, original_filename FROM submitted_files WHERE status = 'processing'"
    )
    if stuck:
        for r in stuck:
            db.execute(
                "UPDATE submitted_files SET status = 'pending', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (r["id"],),
            )
            logger.info("Recovered stuck file #%d: %s", r["id"], r["original_filename"])
        click.echo(f"  Recovered {len(stuck)} file(s) stuck in 'processing' state")


@cli.command()
def process():
    """Process all pending files in the inbox (one-shot)."""
    config = load_config()
    db = get_db(config)
    load_config(db)  # reload with VP names from DB

    inbox_dir = resolve_path(config, "inbox")
    processing_dir = resolve_path(config, "processing")

    # Recover stuck files from previous crash
    _recover_stuck_files(db)

    # 1. INTAKE: scan inbox for new files
    from intake.folder_watcher import scan_existing_files

    click.echo("=== Stage 1: INTAKE ===")
    new_ids = scan_existing_files(inbox_dir, db, processing_dir)
    click.echo(f"  Registered {len(new_ids)} new file(s)")

    # Also pick up any previously pending files
    pending = db.execute(
        "SELECT id FROM submitted_files WHERE status = 'pending' ORDER BY id"
    )
    all_pending_ids = [r["id"] for r in pending]
    click.echo(f"  Total pending: {len(all_pending_ids)} file(s)")

    if not all_pending_ids:
        click.echo("No files to process.")
        _check_and_package(config, db)
        db.close()
        return

    # Initialize Claude backend: prefer Anthropic API, fall back to Claude Code CLI
    client = None
    try:
        import anthropic
        client = anthropic.Anthropic()
        # Reject empty, placeholder, or obviously invalid keys
        key = client.api_key or ""
        if not key or key.startswith("your-") or len(key) < 20:
            raise ValueError("Missing or placeholder API key")
        click.echo("  Using Anthropic API backend")
    except Exception:
        client = None
        click.echo("  Using Claude Code CLI backend (no Anthropic API key)")

    max_workers = config.get("processing", {}).get("max_workers", 1)

    def _process_one(fid):
        """Process a single file, catching errors."""
        # Each thread gets its own DB connection
        thread_db = get_db(config)
        load_config(thread_db)  # load VP names for validation
        try:
            _process_single_file(config, thread_db, client, fid)
        except Exception:
            logger.exception("Error processing file #%d", fid)
            thread_db.execute(
                "UPDATE submitted_files SET status = 'error', error_message = ? WHERE id = ?",
                ("Pipeline error: see logs", fid),
            )
        finally:
            thread_db.close()

    if max_workers > 1 and len(all_pending_ids) > 1:
        click.echo(f"  Processing with {max_workers} worker threads")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_process_one, fid): fid for fid in all_pending_ids}
            for future in as_completed(futures):
                fid = futures[future]
                try:
                    future.result()
                except Exception:
                    logger.exception("Unexpected error in thread for file #%d", fid)
    else:
        for file_id in all_pending_ids:
            try:
                _process_single_file(config, db, client, file_id)
            except Exception:
                logger.exception("Error processing file #%d", file_id)
                db.execute(
                    "UPDATE submitted_files SET status = 'error', error_message = ? WHERE id = ?",
                    ("Pipeline error: see logs", file_id),
                )

    # 5. COMPLETION CHECK
    _check_and_package(config, db)

    db.close()
    click.echo("\nProcessing complete.")


def _process_single_file(config: dict, db: DatabaseManager, client, file_id: int):
    """Run a single file through the full pipeline.

    If client is a valid Anthropic client, uses the API directly.
    If client is None, falls back to Claude Code CLI subprocess.
    """
    from classification.doc_types import CODE_TO_ID, DOC_TYPES, UNCLASSIFIED_FOLDER
    from processing.file_converter import convert_file
    from tracking.counterparty_tracker import find_or_create_counterparty, update_checklist

    row = db.execute("SELECT * FROM submitted_files WHERE id = ?", (file_id,))
    if not row:
        return
    file_info = row[0]
    file_path = Path(file_info["file_path"])

    click.echo(f"\n--- Processing file #{file_id}: {file_info['original_filename']} ---")

    # Mark as processing
    db.execute(
        "UPDATE submitted_files SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (file_id,),
    )

    # 2. CONVERSION
    click.echo("  [2] Converting...")
    scan_dpi = config["processing"]["scan_dpi"]
    max_dim = config["processing"]["max_image_dimension"]
    result = convert_file(file_path, scan_dpi=scan_dpi, max_image_dim=max_dim)

    if result.error:
        db.execute(
            "UPDATE submitted_files SET status = 'error', error_message = ? WHERE id = ?",
            (result.error, file_id),
        )
        click.echo(f"  ERROR: {result.error}")
        return

    db.execute_insert(
        """INSERT INTO processing_log (file_id, stage, action, details)
           VALUES (?, 'conversion', 'converted', ?)""",
        (file_id, json.dumps({"type": result.conversion_type})),
    )

    # 3. CLASSIFICATION
    click.echo("  [3] Classifying with Claude...")
    cls_model = config["claude"]["classification_model"]
    max_retries = config["claude"]["max_retries"]
    retry_delay = config["claude"]["retry_base_delay"]
    max_cls_images = config.get("classification", {}).get("max_classification_images", 5)

    # Build few-shot examples from manual corrections
    from classification.few_shot import get_few_shot_examples
    few_shot_examples = get_few_shot_examples(db)

    if client is not None:
        from classification.classifier import classify_document
        classification = classify_document(
            client,
            text_content=result.text_content,
            image_paths=result.image_paths if result.image_paths else None,
            model=cls_model,
            max_retries=max_retries,
            retry_base_delay=retry_delay,
            max_images=max_cls_images,
            few_shot_examples=few_shot_examples,
        )
    else:
        from classification.claude_code_classifier import classify_with_claude_code
        classification = classify_with_claude_code(
            text_content=result.text_content,
            image_paths=result.image_paths if result.image_paths else None,
            model="sonnet",
            few_shot_examples=few_shot_examples,
        )

    types_str = ", ".join(classification.doc_types)
    click.echo(
        f"  -> Types: [{types_str}] | Company: {classification.company_name} "
        f"| Confidence: {classification.confidence:.2f}"
    )

    # Low confidence → needs review
    threshold = config["classification"]["confidence_threshold"]
    if classification.confidence < threshold:
        click.echo(f"  WARNING: Low confidence ({classification.confidence:.2f} < {threshold})")
        reason = f"Low classification confidence: {classification.confidence:.2f}"
        db.execute(
            "UPDATE submitted_files SET status = 'needs_review', error_message = ? WHERE id = ?",
            (reason, file_id),
        )
        # Notify KYC team
        from notification.notifier import send_review_notification
        send_review_notification(db, file_id, file_info["original_filename"], reason)

    # Unknown company → needs review (cannot assign to any counterparty)
    if not classification.company_name or classification.company_name.lower() == "unknown":
        reason = "Could not identify counterparty from document"
        db.execute(
            "UPDATE submitted_files SET status = 'needs_review', error_message = ? WHERE id = ?",
            (reason, file_id),
        )
        click.echo(f"  -> Unknown company, flagged for manual review")
        from notification.notifier import send_review_notification
        send_review_notification(db, file_id, file_info["original_filename"], reason)
        return

    # Find or create counterparty
    fuzzy_threshold = config["classification"]["fuzzy_match_threshold"]
    counterparty_id = find_or_create_counterparty(
        db, classification.company_name, fuzzy_threshold=fuzzy_threshold
    )

    # --- Handle unknown / unclassified doc types ---
    primary_type = classification.primary_doc_type
    is_unknown = (classification.doc_types == ["unknown"])

    if is_unknown:
        # File belongs to a company but doesn't match any KYC doc type
        # → move to 00_unclassified folder
        cp_rows = db.execute("SELECT slug FROM counterparties WHERE id = ?", (counterparty_id,))
        slug = cp_rows[0]["slug"] if cp_rows else "_unassigned"
        classified_dir = resolve_path(config, "classified")
        dest_dir = classified_dir / slug / UNCLASSIFIED_FOLDER
        dest_dir.mkdir(parents=True, exist_ok=True)
        dest_path = dest_dir / file_path.name
        if not dest_path.exists():
            shutil.move(str(file_path), str(dest_path))
        db.execute(
            "UPDATE submitted_files SET file_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
            (str(dest_path), file_id),
        )

        # Save classification record (doc_type_id=NULL for unknown)
        cls_id = db.execute_insert(
            """INSERT OR REPLACE INTO document_classifications
               (file_id, doc_type_id, counterparty_id, detected_company_name,
                confidence, is_primary, model_used, input_tokens, output_tokens, raw_response)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                file_id, None, counterparty_id,
                classification.company_name, classification.confidence,
                1, classification.model_used,
                classification.input_tokens, classification.output_tokens,
                classification.raw_response,
            ),
        )

        reason = f"Unclassified document — copied to {slug}/{UNCLASSIFIED_FOLDER}/"
        db.execute(
            "UPDATE submitted_files SET status = 'needs_review', error_message = ? WHERE id = ?",
            (reason, file_id),
        )

        # Notify KYC team
        from notification.notifier import send_review_notification
        send_review_notification(db, file_id, file_info["original_filename"], reason)

        click.echo(f"  -> Unknown type, copied to {slug}/{UNCLASSIFIED_FOLDER}/")
        click.echo(f"  Skipping extraction for unclassified document")
        return

    # --- Normal path: known doc types ---

    # Clean up stale data from prior failed attempts
    # First clear old checklist references so they don't point to stale data
    db.execute(
        """UPDATE counterparty_checklist
           SET status = 'missing', file_id = NULL, updated_at = CURRENT_TIMESTAMP
           WHERE file_id = ?""",
        (file_id,),
    )
    db.execute("DELETE FROM extraction_results WHERE file_id = ?", (file_id,))
    db.execute("DELETE FROM document_classifications WHERE file_id = ?", (file_id,))

    # Save classification records for ALL applicable doc_types
    classification_ids = {}  # doc_type_code -> classification_id
    primary_dest = file_path  # track file location after move
    for i, doc_type_code in enumerate(classification.doc_types):
        doc_type_id = CODE_TO_ID.get(doc_type_code)
        is_primary = 1 if i == 0 else 0
        cls_id = db.execute_insert(
            """INSERT OR REPLACE INTO document_classifications
               (file_id, doc_type_id, counterparty_id, detected_company_name,
                confidence, is_primary, model_used, input_tokens, output_tokens, raw_response)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                file_id, doc_type_id, counterparty_id,
                classification.company_name, classification.confidence,
                is_primary, classification.model_used,
                classification.input_tokens, classification.output_tokens,
                classification.raw_response,
            ),
        )
        classification_ids[doc_type_code] = cls_id

        # Move/copy file to classified directory for each type
        if doc_type_id and doc_type_code in DOC_TYPES:
            cp_rows = db.execute("SELECT slug FROM counterparties WHERE id = ?", (counterparty_id,))
            if cp_rows:
                classified_dir = resolve_path(config, "classified")
                doc_type_info = DOC_TYPES[doc_type_code]
                dest_dir = classified_dir / cp_rows[0]["slug"] / doc_type_info.folder_name
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest_path = dest_dir / file_path.name
                if not dest_path.exists():
                    if is_primary:
                        shutil.move(str(file_path), str(dest_path))
                    else:
                        # Multi-type: primary was already moved, copy from new location
                        shutil.copy2(str(primary_dest), str(dest_path))
                if is_primary:
                    primary_dest = dest_path

    db.execute(
        "UPDATE submitted_files SET status = 'classified', file_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (str(primary_dest), file_id),
    )

    # 4. EXTRACTION — run for ALL applicable doc_types
    from classification.prompts import EXTRACTION_PROMPTS
    max_ext_images = config.get("classification", {}).get("max_extraction_images", 10)
    extractable_types = [t for t in classification.doc_types if t in EXTRACTION_PROMPTS]
    click.echo(f"  [4] Extracting structured data for {len(extractable_types)} type(s): {extractable_types}")

    for ext_type in extractable_types:
        click.echo(f"  [4] Extracting: {ext_type}...")
        if client is not None:
            from extraction.extractor import extract_document_data
            extraction = extract_document_data(
                client,
                doc_type=ext_type,
                text_content=result.text_content,
                image_paths=result.image_paths if result.image_paths else None,
                model_simple=config["claude"]["extraction_model_simple"],
                model_complex=config["claude"]["extraction_model_complex"],
                max_retries=max_retries,
                retry_base_delay=retry_delay,
                max_images=max_ext_images,
            )
        else:
            from extraction.claude_code_extractor import extract_with_claude_code
            extraction = extract_with_claude_code(
                doc_type=ext_type,
                text_content=result.text_content,
                image_paths=result.image_paths if result.image_paths else None,
                model="sonnet",
            )

        if extraction.validation_errors:
            click.echo(f"    Validation warnings ({ext_type}): {extraction.validation_errors}")

        # Save extraction (linked to its classification)
        cls_id = classification_ids.get(ext_type)
        db.execute_insert(
            """INSERT OR REPLACE INTO extraction_results
               (file_id, classification_id, extracted_data, validation_passed,
                validation_errors, model_used, input_tokens, output_tokens, raw_response)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                file_id, cls_id,
                json.dumps(extraction.extracted_data, ensure_ascii=False),
                1 if extraction.validated else 0,
                json.dumps(extraction.validation_errors),
                extraction.model_used,
                extraction.input_tokens, extraction.output_tokens,
                extraction.raw_response,
            ),
        )
        click.echo(f"    {ext_type}: validated={extraction.validated}")

    db.execute(
        "UPDATE submitted_files SET status = 'extraction_done', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (file_id,),
    )

    # Clean up conversion artifacts from classified directory
    _cleanup_conversion_artifacts(result.image_paths, primary_dest)

    # Update checklist for ALL applicable doc_types
    for doc_type_code in classification.doc_types:
        doc_type_id = CODE_TO_ID.get(doc_type_code)
        if doc_type_id and counterparty_id:
            update_checklist(db, counterparty_id, doc_type_id, file_id)

    # If counterparty was already packaged, reset to trigger re-packaging
    _reset_if_completed(db, counterparty_id)

    click.echo(f"  Done. Types: [{types_str}] | Extracted: {len(extractable_types)} type(s)")


def _reset_if_completed(db, counterparty_id: int):
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


def _cleanup_conversion_artifacts(image_paths: list | None, original_path):
    """Remove temporary image files generated during conversion."""
    if not image_paths:
        return
    original_path = Path(original_path)
    for img in image_paths:
        try:
            img = Path(img)
            if img.exists() and img.resolve() != original_path.resolve():
                img.unlink()
                logger.debug("Cleaned up conversion artifact: %s", img)
        except Exception as e:
            logger.warning("Could not remove conversion artifact %s: %s", img, e)


def _check_and_package(config: dict, db: DatabaseManager):
    """Check all counterparties for completion and package if ready."""
    from notification.notifier import send_completion_notification
    from tracking.completion_checker import get_newly_completed
    from tracking.packaging import package_counterparty

    click.echo("\n=== Stage 5: COMPLETION CHECK ===")
    newly_completed = get_newly_completed(db)

    if not newly_completed:
        click.echo("  No newly completed counterparties.")
        return

    classified_dir = resolve_path(config, "classified")
    completed_dir = resolve_path(config, "completed")

    smtp_config = {
        "host": config["notification"]["smtp_host"],
        "port": config["notification"]["smtp_port"],
        "user": config["notification"]["smtp_user"],
        "password": config["notification"]["smtp_password"],
        "from_address": config["notification"]["from_address"],
        "recipients": config["notification"]["compliance_team"],
    }

    for cp_id in newly_completed:
        click.echo(f"  Packaging counterparty #{cp_id}...")
        zip_path = package_counterparty(db, cp_id, classified_dir, completed_dir)
        if zip_path:
            click.echo(f"  -> Package: {zip_path}")
            send_completion_notification(db, cp_id, zip_path, smtp_config)


@cli.command()
def watch():
    """Start watching the inbox folder for new files (continuous).

    Automatically processes files through the full pipeline when detected.
    """
    config = load_config()
    db = get_db(config)

    inbox_dir = resolve_path(config, "inbox")
    processing_dir = resolve_path(config, "processing")
    supported_ext = set(config.get("processing", {}).get(
        "supported_extensions", [".pdf", ".docx", ".xlsx", ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp"]
    ))

    from intake.folder_watcher import start_watching

    # Recover any files stuck in 'processing' from a previous crash
    _recover_stuck_files(db)

    click.echo(f"Watching inbox: {inbox_dir}")
    click.echo("Press Ctrl+C to stop.\n")

    client = None
    try:
        import anthropic
        client = anthropic.Anthropic()
        if not client.api_key:
            raise ValueError("Empty API key")
        click.echo("Using Anthropic API backend\n")
    except Exception:
        client = None
        click.echo("Using Claude Code CLI backend (no Anthropic API key)\n")

    def on_new_file(file_id):
        click.echo(f"\nNew file registered: #{file_id}")
        try:
            _process_single_file(config, db, client, file_id)
            _check_and_package(config, db)
        except Exception:
            logger.exception("Error processing file #%d", file_id)
            db.execute(
                "UPDATE submitted_files SET status = 'error', error_message = ? WHERE id = ?",
                ("Pipeline error: see logs", file_id),
            )

    observer = start_watching(
        inbox_dir, db, processing_dir,
        callback=on_new_file,
        supported_extensions=supported_ext,
    )

    # Periodically check for completed counterparties (e.g. after web assign)
    check_interval = config.get("watch", {}).get("poll_interval", 5)
    package_check_interval = 30  # seconds between completion checks
    seconds_since_check = 0

    try:
        while True:
            time.sleep(check_interval)
            seconds_since_check += check_interval
            if seconds_since_check >= package_check_interval:
                seconds_since_check = 0
                try:
                    _check_and_package(config, db)
                except Exception:
                    logger.exception("Error during periodic completion check")
    except KeyboardInterrupt:
        click.echo("\nStopping watcher...")
        observer.stop()
    observer.join()
    db.close()


@cli.command()
def status():
    """Show current status of all counterparties."""
    config = load_config()
    db = get_db(config)

    from tracking.completion_checker import get_all_counterparty_statuses

    statuses = get_all_counterparty_statuses(db)

    if not statuses:
        click.echo("No counterparties registered yet.")
        db.close()
        return

    click.echo(f"\n{'='*80}")
    click.echo(f"{'COUNTERPARTY KYC STATUS':^80}")
    click.echo(f"{'='*80}\n")

    for s in statuses:
        marker = "[COMPLETE]" if s["is_complete"] else "[IN PROGRESS]"
        click.echo(f"  {s['name']} {marker}  ({s['progress']})")

        for doc in s["documents"]:
            icon = "+" if doc["status"] in ("received", "verified") else "-"
            req = "*" if doc["required"] else " "
            file_info = f" <- {doc['file']}" if doc.get("file") else ""
            shared_info = ""
            if doc.get("also_serves"):
                shared_info = f" (also: {', '.join(doc['also_serves'])})"
            click.echo(f"    [{icon}]{req} {doc['name']}{file_info}{shared_info}")
        click.echo()

    # Summary
    total = len(statuses)
    completed = sum(1 for s in statuses if s["is_complete"])
    click.echo(f"Total: {total} | Completed: {completed} | In Progress: {total - completed}")

    # Token usage summary
    token_rows = db.execute(
        """SELECT SUM(input_tokens) as total_in, SUM(output_tokens) as total_out
           FROM document_classifications"""
    )
    ext_rows = db.execute(
        """SELECT SUM(input_tokens) as total_in, SUM(output_tokens) as total_out
           FROM extraction_results"""
    )
    cls_in = token_rows[0]["total_in"] or 0
    cls_out = token_rows[0]["total_out"] or 0
    ext_in = ext_rows[0]["total_in"] or 0
    ext_out = ext_rows[0]["total_out"] or 0
    click.echo(
        f"\nAPI Usage - Classification: {cls_in:,}in/{cls_out:,}out tokens | "
        f"Extraction: {ext_in:,}in/{ext_out:,}out tokens"
    )

    db.close()


@cli.command()
def report():
    """Generate a weekly status report."""
    config = load_config()
    db = get_db(config)

    from reporting.weekly_report import generate_weekly_report

    reports_dir = resolve_path(config, "reports")
    report_path = generate_weekly_report(db, reports_dir)
    click.echo(f"Report generated: {report_path}")

    db.close()


@cli.command()
@click.argument("file_id", type=int)
def reprocess(file_id):
    """Reprocess a specific file by ID."""
    config = load_config()
    db = get_db(config)

    # Check file exists
    row = db.execute("SELECT * FROM submitted_files WHERE id = ?", (file_id,))
    if not row:
        click.echo(f"File #{file_id} not found.")
        db.close()
        return

    file_info = row[0]
    click.echo(f"Reprocessing file #{file_id}: {file_info['original_filename']}")

    # Reset status
    db.execute(
        "UPDATE submitted_files SET status = 'pending', error_message = NULL WHERE id = ?",
        (file_id,),
    )

    # Clear previous results
    db.execute("DELETE FROM document_classifications WHERE file_id = ?", (file_id,))
    db.execute("DELETE FROM extraction_results WHERE file_id = ?", (file_id,))

    # Initialize backend
    client = None
    try:
        import anthropic
        client = anthropic.Anthropic()
        if not client.api_key:
            raise ValueError("Empty API key")
        click.echo("Using Anthropic API backend")
    except Exception:
        client = None
        click.echo("Using Claude Code CLI backend")

    try:
        _process_single_file(config, db, client, file_id)
        click.echo("Reprocessing complete.")
    except Exception:
        logger.exception("Error reprocessing file #%d", file_id)
        click.echo("Reprocessing failed. Check logs for details.")

    db.close()


@cli.command()
def files():
    """List all submitted files and their processing status."""
    config = load_config()
    db = get_db(config)

    rows = db.execute(
        """SELECT sf.id, sf.original_filename, sf.status, sf.error_message,
                  GROUP_CONCAT(dt.code, ', ') as doc_types,
                  cp.name as counterparty
           FROM submitted_files sf
           LEFT JOIN document_classifications dc ON sf.id = dc.file_id
           LEFT JOIN kyc_doc_types dt ON dc.doc_type_id = dt.id
           LEFT JOIN counterparties cp ON dc.counterparty_id = cp.id
           GROUP BY sf.id
           ORDER BY sf.id"""
    )

    if not rows:
        click.echo("No files submitted yet.")
        db.close()
        return

    click.echo(f"\n{'ID':>4}  {'Status':14s}  {'Doc Types':30s}  {'Filename'}")
    click.echo("-" * 90)
    for r in rows:
        doc_types = r["doc_types"] or "-"
        err = f" [{r['error_message']}]" if r["error_message"] else ""
        click.echo(f"#{r['id']:>3}  {r['status']:14s}  {doc_types:30s}  {r['original_filename']}{err}")

    db.close()


@cli.command()
def review():
    """List files that need manual review (low confidence or errors)."""
    config = load_config()
    db = get_db(config)

    rows = db.execute(
        """SELECT sf.id, sf.original_filename, sf.status, sf.error_message, sf.created_at
           FROM submitted_files sf
           WHERE sf.status IN ('needs_review', 'error')
           ORDER BY sf.created_at DESC"""
    )

    if not rows:
        click.echo("No files need review.")
        db.close()
        return

    click.echo(f"\n{'ID':>4}  {'Status':14s}  {'Created':20s}  {'Filename'}")
    click.echo("-" * 90)
    for r in rows:
        click.echo(f"#{r['id']:>3}  {r['status']:14s}  {r['created_at']:20s}  {r['original_filename']}")
        if r["error_message"]:
            click.echo(f"      Reason: {r['error_message']}")

    click.echo(f"\nUse 'python main.py assign <file_id> <doc_type> [--counterparty NAME]' to manually classify.")
    click.echo(f"Use 'python main.py reprocess <file_id>' to retry with Claude.")

    db.close()


@cli.command()
@click.argument("file_id", type=int)
@click.argument("doc_type_codes", nargs=-1, required=True)
@click.option("--counterparty", "-c", default=None, help="Counterparty name (fuzzy matched or created)")
def assign(file_id, doc_type_codes, counterparty):
    """Manually assign document type(s) and counterparty to a file.

    Supports multiple doc types: python main.py assign 2 incorporation maa

    This bypasses Claude classification and directly updates the checklist.
    """
    from classification.doc_types import CODE_TO_ID, DOC_TYPES
    from processing.file_converter import convert_file
    from tracking.counterparty_tracker import find_or_create_counterparty, update_checklist

    config = load_config()
    db = get_db(config)

    # Validate file exists
    row = db.execute("SELECT * FROM submitted_files WHERE id = ?", (file_id,))
    if not row:
        click.echo(f"File #{file_id} not found.")
        db.close()
        return
    file_info = row[0]

    # Validate doc types
    for code in doc_type_codes:
        if code not in DOC_TYPES:
            click.echo(f"Unknown document type: '{code}'")
            click.echo(f"Valid types: {', '.join(sorted(DOC_TYPES.keys()))}")
            db.close()
            return

    # Find or create counterparty
    if not counterparty:
        # Try to get from existing classification
        existing = db.execute(
            "SELECT detected_company_name FROM document_classifications WHERE file_id = ? LIMIT 1",
            (file_id,),
        )
        if existing:
            counterparty = existing[0]["detected_company_name"]
        else:
            click.echo("Please specify counterparty name with --counterparty / -c")
            db.close()
            return

    fuzzy_threshold = config["classification"]["fuzzy_match_threshold"]
    counterparty_id = find_or_create_counterparty(db, counterparty, fuzzy_threshold=fuzzy_threshold)

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

    # Clear old checklist references before replacing classifications
    db.execute(
        """UPDATE counterparty_checklist
           SET status = 'missing', file_id = NULL, updated_at = CURRENT_TIMESTAMP
           WHERE file_id = ?""",
        (file_id,),
    )

    # Clear previous classifications and extraction results
    db.execute("DELETE FROM extraction_results WHERE file_id = ?", (file_id,))
    db.execute("DELETE FROM document_classifications WHERE file_id = ?", (file_id,))

    # Save classification for each doc type
    current_path = Path(file_info["file_path"])
    primary_cls_id = None
    for i, code in enumerate(doc_type_codes):
        doc_type_id = CODE_TO_ID[code]
        is_primary = 1 if i == 0 else 0

        cls_id = db.execute_insert(
            """INSERT OR REPLACE INTO document_classifications
               (file_id, doc_type_id, counterparty_id, detected_company_name,
                confidence, is_primary, model_used, input_tokens, output_tokens, raw_response)
               VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?)""",
            (file_id, doc_type_id, counterparty_id, counterparty,
             1.0, is_primary, "manual",
             f"Manually assigned: {', '.join(doc_type_codes)}"),
        )
        if i == 0:
            primary_cls_id = cls_id

        # Update checklist
        update_checklist(db, counterparty_id, doc_type_id, file_id)

        # Move/copy to classified directory
        cp_rows = db.execute("SELECT slug FROM counterparties WHERE id = ?", (counterparty_id,))
        if cp_rows:
            classified_dir = resolve_path(config, "classified")
            doc_type_info = DOC_TYPES[code]
            dest_dir = classified_dir / cp_rows[0]["slug"] / doc_type_info.folder_name
            dest_dir.mkdir(parents=True, exist_ok=True)
            dest = dest_dir / current_path.name
            if current_path.exists() and not dest.exists():
                if i == 0:
                    shutil.move(str(current_path), str(dest))
                    current_path = dest
                else:
                    shutil.copy2(str(current_path), str(dest))

    # Update file status and path
    db.execute(
        "UPDATE submitted_files SET status = 'classified', file_path = ?, error_message = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (str(current_path), file_id),
    )

    types_str = ", ".join(doc_type_codes)
    click.echo(f"Assigned file #{file_id} ({file_info['original_filename']}):")
    click.echo(f"  Types: [{types_str}]")
    click.echo(f"  Counterparty: {counterparty} (#{counterparty_id})")

    # Run extraction on primary doc type
    primary_type = doc_type_codes[0]
    from classification.prompts import EXTRACTION_PROMPTS
    if primary_type in EXTRACTION_PROMPTS:
        click.echo(f"  Extracting structured data (type: {primary_type})...")
        try:
            scan_dpi = config["processing"]["scan_dpi"]
            max_dim = config["processing"]["max_image_dimension"]
            result = convert_file(current_path, scan_dpi=scan_dpi, max_image_dim=max_dim)

            if result.error:
                click.echo(f"  Extraction skipped (conversion error): {result.error}")
            else:
                client = None
                try:
                    import anthropic
                    client = anthropic.Anthropic()
                    if not client.api_key:
                        raise ValueError("Empty")
                except Exception:
                    client = None

                max_ext_images = config.get("classification", {}).get("max_extraction_images", 10)
                if client is not None:
                    from extraction.extractor import extract_document_data
                    extraction = extract_document_data(
                        client,
                        doc_type=primary_type,
                        text_content=result.text_content,
                        image_paths=result.image_paths if result.image_paths else None,
                        model_simple=config["claude"]["extraction_model_simple"],
                        model_complex=config["claude"]["extraction_model_complex"],
                        max_retries=config["claude"]["max_retries"],
                        retry_base_delay=config["claude"]["retry_base_delay"],
                        max_images=max_ext_images,
                    )
                else:
                    from extraction.claude_code_extractor import extract_with_claude_code
                    extraction = extract_with_claude_code(
                        doc_type=primary_type,
                        text_content=result.text_content,
                        image_paths=result.image_paths if result.image_paths else None,
                        model="sonnet",
                    )

                db.execute_insert(
                    """INSERT OR REPLACE INTO extraction_results
                       (file_id, classification_id, extracted_data, validation_passed,
                        validation_errors, model_used, input_tokens, output_tokens, raw_response)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        file_id, primary_cls_id,
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
                click.echo(f"  Extraction done (validated: {extraction.validated})")

                # Clean up conversion artifacts
                _cleanup_conversion_artifacts(result.image_paths, current_path)
        except Exception as e:
            click.echo(f"  Extraction failed: {e}")
            logger.exception("Extraction failed for file #%d", file_id)

    # If counterparty was already packaged, reset to trigger re-packaging
    _reset_if_completed(db, counterparty_id)

    # Check completion and auto-package
    _check_and_package(config, db)

    db.close()


@cli.command()
def package():
    """Check for completed counterparties and package them.

    Scans all in-progress counterparties, packages those with all required
    documents, and sends notifications. Safe to run multiple times — already
    packaged counterparties (status='completed') are skipped.
    """
    config = load_config()
    db = get_db(config)
    _check_and_package(config, db)
    db.close()


@cli.command()
@click.option("--port", default=5050, type=int, help="Port for the web settings interface")
def settings(port):
    """Launch the web settings interface."""
    config = load_config()
    db_path = str(PROJECT_ROOT / config["paths"]["database"])

    from web import create_app

    app = create_app(db_path=db_path)

    url = f"http://127.0.0.1:{port}"
    click.echo(f"Starting settings interface at {url}")

    import webbrowser
    webbrowser.open(url)

    app.run(host="0.0.0.0", port=port, debug=False)


if __name__ == "__main__":
    cli()
