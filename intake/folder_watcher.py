"""Watchdog-based folder monitor for incoming KYC files."""

import logging
import time
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from database.connection import DatabaseManager
from intake.file_registry import register_file

logger = logging.getLogger(__name__)


DEFAULT_SUPPORTED_EXTENSIONS = {
    ".pdf", ".docx", ".xlsx",
    ".png", ".jpg", ".jpeg", ".tiff", ".tif", ".bmp",
}


class InboxHandler(FileSystemEventHandler):
    """Handle new files appearing in the inbox directory."""

    def __init__(self, db: DatabaseManager, processing_dir: Path,
                 callback=None, supported_extensions=None):
        self.db = db
        self.processing_dir = processing_dir
        self.callback = callback  # called with file_id after registration
        self.supported_extensions = supported_extensions or DEFAULT_SUPPORTED_EXTENSIONS
        self._pending = set()

    def on_created(self, event):
        if event.is_directory:
            return
        self._handle_file(Path(event.src_path))

    def on_moved(self, event):
        if event.is_directory:
            return
        self._handle_file(Path(event.dest_path))

    def _handle_file(self, file_path: Path):
        if file_path.suffix.lower() not in self.supported_extensions:
            logger.debug("Ignoring unsupported file: %s", file_path.name)
            return

        # Wait briefly for file to finish writing
        if file_path in self._pending:
            return
        self._pending.add(file_path)

        try:
            # Wait for file to stabilize (no size change)
            _wait_for_stable(file_path)

            file_id = register_file(self.db, file_path, self.processing_dir)
            if file_id and self.callback:
                self.callback(file_id)
        except Exception:
            logger.exception("Error processing incoming file: %s", file_path.name)
        finally:
            self._pending.discard(file_path)


def _wait_for_stable(file_path: Path, interval: float = 0.5, checks: int = 3):
    """Wait until file size stabilizes (file finished writing)."""
    prev_size = -1
    stable_count = 0
    for _ in range(checks * 10):
        if not file_path.exists():
            return
        size = file_path.stat().st_size
        if size == prev_size and size > 0:
            stable_count += 1
            if stable_count >= checks:
                return
        else:
            stable_count = 0
        prev_size = size
        time.sleep(interval)


def start_watching(
    inbox_dir: Path,
    db: DatabaseManager,
    processing_dir: Path,
    callback=None,
    supported_extensions=None,
) -> Observer:
    """Start watching the inbox directory for new files.

    Returns the Observer instance (call .stop() to stop).
    """
    inbox_dir.mkdir(parents=True, exist_ok=True)

    handler = InboxHandler(db, processing_dir, callback=callback,
                           supported_extensions=supported_extensions)
    observer = Observer()
    observer.schedule(handler, str(inbox_dir), recursive=False)
    observer.start()

    logger.info("Watching inbox: %s", inbox_dir)
    return observer


def scan_existing_files(
    inbox_dir: Path,
    db: DatabaseManager,
    processing_dir: Path,
    supported_extensions=None,
) -> list[int]:
    """Scan inbox for any existing files and register them.

    Returns list of newly registered file IDs.
    """
    supported = supported_extensions or DEFAULT_SUPPORTED_EXTENSIONS
    file_ids = []

    for file_path in sorted(inbox_dir.iterdir()):
        if file_path.is_file() and file_path.suffix.lower() in supported:
            file_id = register_file(db, file_path, processing_dir)
            if file_id:
                file_ids.append(file_id)

    return file_ids
