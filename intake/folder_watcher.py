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
                 callback=None, supported_extensions=None, inbox_dir: Path | None = None,
                 max_depth: int = 3):
        super().__init__()
        self.db = db
        self.processing_dir = processing_dir
        self.callback = callback
        self.supported_extensions = supported_extensions or DEFAULT_SUPPORTED_EXTENSIONS
        self._pending = set()
        self._inbox_dir = inbox_dir
        self._max_depth = max_depth

    def _within_depth(self, file_path: Path) -> bool:
        """Check if file is within the allowed subfolder depth."""
        if self._inbox_dir is None:
            return True
        try:
            rel = file_path.relative_to(self._inbox_dir)
            # rel.parts count: file in inbox = 1, one subfolder = 2, etc.
            return len(rel.parts) <= self._max_depth + 1
        except ValueError:
            return False

    def on_created(self, event):
        if event.is_directory:
            return
        file_path = Path(event.src_path)
        if self._within_depth(file_path):
            self._handle_file(file_path)

    def on_moved(self, event):
        if event.is_directory:
            return
        file_path = Path(event.dest_path)
        if self._within_depth(file_path):
            self._handle_file(file_path)

    def _handle_file(self, file_path: Path):
        # Skip Windows Zone.Identifier metadata files (created when copying from Windows to WSL)
        if "Zone.Identifier" in file_path.name:
            logger.debug("Ignoring Zone.Identifier file: %s", file_path.name)
            return

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
    max_depth: int = 3,
) -> Observer:
    """Start watching the inbox directory for new files.

    Monitors inbox and up to max_depth levels of subdirectories.
    Returns the Observer instance (call .stop() to stop).
    """
    inbox_dir.mkdir(parents=True, exist_ok=True)

    handler = InboxHandler(db, processing_dir, callback=callback,
                           supported_extensions=supported_extensions,
                           inbox_dir=inbox_dir, max_depth=max_depth)
    observer = Observer()
    observer.schedule(handler, str(inbox_dir), recursive=True)
    observer.start()

    logger.info("Watching inbox (recursive, max_depth=%d): %s", max_depth, inbox_dir)
    return observer


def scan_existing_files(
    inbox_dir: Path,
    db: DatabaseManager,
    processing_dir: Path,
    supported_extensions=None,
    max_depth: int = 3,
) -> list[int]:
    """Scan inbox and subdirectories (up to max_depth levels) for existing files.

    Returns list of newly registered file IDs.
    """
    supported = supported_extensions or DEFAULT_SUPPORTED_EXTENSIONS
    file_ids = []

    for file_path in sorted(inbox_dir.rglob("*")):
        if not file_path.is_file():
            continue
        # Check depth: relative parts count (filename = 1 part, one subfolder = 2, etc.)
        try:
            rel = file_path.relative_to(inbox_dir)
            if len(rel.parts) > max_depth + 1:
                continue
        except ValueError:
            continue
        if "Zone.Identifier" in file_path.name:
            continue
        if file_path.suffix.lower() in supported:
            file_id = register_file(db, file_path, processing_dir)
            if file_id:
                file_ids.append(file_id)

    return file_ids
