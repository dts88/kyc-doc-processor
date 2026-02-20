"""SQLite connection manager with WAL mode."""

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path


class DatabaseManager:
    """Thread-safe SQLite connection manager."""

    _local = threading.local()

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def _get_connection(self) -> sqlite3.Connection:
        if not hasattr(self._local, "connection") or self._local.connection is None:
            conn = sqlite3.connect(str(self.db_path))
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            self._local.connection = conn
        return self._local.connection

    @contextmanager
    def get_cursor(self):
        """Get a database cursor within a transaction."""
        conn = self._get_connection()
        cursor = conn.cursor()
        try:
            yield cursor
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def execute(self, sql: str, params=None):
        """Execute a single SQL statement and return results."""
        with self.get_cursor() as cursor:
            cursor.execute(sql, params or ())
            return cursor.fetchall()

    def execute_insert(self, sql: str, params=None) -> int:
        """Execute an INSERT and return the last row id."""
        with self.get_cursor() as cursor:
            cursor.execute(sql, params or ())
            return cursor.lastrowid

    def execute_script(self, sql_script: str):
        """Execute a multi-statement SQL script."""
        conn = self._get_connection()
        conn.executescript(sql_script)

    def close(self):
        if hasattr(self._local, "connection") and self._local.connection:
            self._local.connection.close()
            self._local.connection = None
