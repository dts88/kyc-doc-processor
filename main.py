"""KYC Document Processor - CLI Entry Point & Pipeline Coordinator."""

import json
import logging
import shutil
import sys
import time
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

    # Process each file through the pipeline
    import anthropic

    client = anthropic.Anthropic()

    for file_id in all_pending_ids:
        try:
            _process_single_file(config, db, client, file_id)
        except Exception:
            logger.exception("Error processing file #%d", file_id)
            db.execute(
                "UPDATE submitted_files SET status = 'error', error_message = ? WHERE id = ?",
                (f"Pipeline error: see logs", file_id),
            )

    # 5. COMPLETION CHECK
    _check_and_package(config, db)

    db.close()
    click.echo("\nProcessing complete.")


def _process_single_file(config: dict, db: DatabaseManager, client, file_id: int):
    """Run a single file through the full pipeline."""
    from classification.classifier import classify_document
    from classification.doc_types import CODE_TO_ID, DOC_TYPES
    from extraction.extractor import extract_document_data
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
    classification = classify_document(
        client,
        text_content=result.text_content,
        image_paths=result.image_paths if result.image_paths else None,
        model=cls_model,
        max_retries=max_retries,
        retry_base_delay=retry_delay,
        max_images=max_cls_images,
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

    # Find or create counterparty
    fuzzy_threshold = config["classification"]["fuzzy_match_threshold"]
    counterparty_id = find_or_create_counterparty(
        db, classification.company_name, fuzzy_threshold=fuzzy_threshold
    )

    # Save classification records for ALL applicable doc_types
    classification_ids = {}  # doc_type_code -> classification_id
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

        # Copy file to classified directory for each type
        if doc_type_id and doc_type_code in DOC_TYPES:
            cp_rows = db.execute("SELECT slug FROM counterparties WHERE id = ?", (counterparty_id,))
            if cp_rows:
                classified_dir = resolve_path(load_config(), "classified")
                doc_type_info = DOC_TYPES[doc_type_code]
                dest_dir = classified_dir / cp_rows[0]["slug"] / doc_type_info.folder_name
                dest_dir.mkdir(parents=True, exist_ok=True)
                dest_path = dest_dir / file_path.name
                if not dest_path.exists():
                    shutil.copy2(str(file_path), str(dest_path))

    db.execute(
        "UPDATE submitted_files SET status = 'classified', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (file_id,),
    )

    # 4. EXTRACTION — run for primary doc_type only
    primary_type = classification.primary_doc_type
    if primary_type == "unknown":
        click.echo("  Skipping extraction for unknown document type")
        return

    click.echo(f"  [4] Extracting structured data (primary type: {primary_type})...")
    max_ext_images = config.get("classification", {}).get("max_extraction_images", 10)
    extraction = extract_document_data(
        client,
        doc_type=primary_type,
        text_content=result.text_content,
        image_paths=result.image_paths if result.image_paths else None,
        model_simple=config["claude"]["extraction_model_simple"],
        model_complex=config["claude"]["extraction_model_complex"],
        max_retries=max_retries,
        retry_base_delay=retry_delay,
        max_images=max_ext_images,
    )

    if extraction.validation_errors:
        click.echo(f"  Validation warnings: {extraction.validation_errors}")

    # Save extraction (linked to primary classification)
    primary_cls_id = classification_ids.get(primary_type)
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

    # Update checklist for ALL applicable doc_types
    for doc_type_code in classification.doc_types:
        doc_type_id = CODE_TO_ID.get(doc_type_code)
        if doc_type_id and counterparty_id:
            update_checklist(db, counterparty_id, doc_type_id, file_id)

    click.echo(f"  Done. Types: [{types_str}] | Extraction validated: {extraction.validated}")


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

    import anthropic
    try:
        client = anthropic.Anthropic()
    except Exception:
        client = None
        click.echo("WARNING: Anthropic API key not configured. Files will be registered but not auto-processed.")
        click.echo("         Use 'python main.py process' manually after configuring the API key.\n")

    def on_new_file(file_id):
        click.echo(f"\nNew file registered: #{file_id}")
        if client is None:
            click.echo(f"  Skipping auto-processing (no API key)")
            return
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

    try:
        while True:
            time.sleep(1)
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

    # Reprocess
    import anthropic

    client = anthropic.Anthropic()
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

    # Save classification for each doc type
    for i, code in enumerate(doc_type_codes):
        doc_type_id = CODE_TO_ID[code]
        is_primary = 1 if i == 0 else 0

        db.execute_insert(
            """INSERT OR REPLACE INTO document_classifications
               (file_id, doc_type_id, counterparty_id, detected_company_name,
                confidence, is_primary, model_used, input_tokens, output_tokens, raw_response)
               VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0, ?)""",
            (file_id, doc_type_id, counterparty_id, counterparty,
             1.0, is_primary, "manual",
             f"Manually assigned: {', '.join(doc_type_codes)}"),
        )

        # Update checklist
        update_checklist(db, counterparty_id, doc_type_id, file_id)

        # Copy to classified directory
        cp_rows = db.execute("SELECT slug FROM counterparties WHERE id = ?", (counterparty_id,))
        if cp_rows:
            classified_dir = resolve_path(config, "classified")
            doc_type_info = DOC_TYPES[code]
            dest_dir = classified_dir / cp_rows[0]["slug"] / doc_type_info.folder_name
            dest_dir.mkdir(parents=True, exist_ok=True)
            src = Path(file_info["file_path"])
            dest = dest_dir / src.name
            if src.exists() and not dest.exists():
                shutil.copy2(str(src), str(dest))

    # Update file status
    db.execute(
        "UPDATE submitted_files SET status = 'classified', error_message = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
        (file_id,),
    )

    types_str = ", ".join(doc_type_codes)
    click.echo(f"Assigned file #{file_id} ({file_info['original_filename']}):")
    click.echo(f"  Types: [{types_str}]")
    click.echo(f"  Counterparty: {counterparty} (#{counterparty_id})")

    # Check completion
    from tracking.completion_checker import check_completion
    if check_completion(db, counterparty_id):
        click.echo(f"  ** Counterparty #{counterparty_id} is now COMPLETE! Run 'python main.py process' to package.")

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

    app.run(host="127.0.0.1", port=port, debug=False)


if __name__ == "__main__":
    cli()
