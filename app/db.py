"""Database access for the reconciliation app (SQLite via the stdlib driver).

The app deliberately uses plain SQL instead of an ORM.  That keeps the Docker
image small, makes the schema easy to inspect, and lets every write happen in an
explicit transaction so that a failed publish leaves the previous published
snapshot untouched.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS datasets (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT    NOT NULL,
    created_at      TEXT    NOT NULL,
    -- Monotonically increasing input version.  Any upload that can change the
    -- result of a calculation bumps this value, which invalidates previews.
    input_version   INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id      INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    input_version   INTEGER NOT NULL,
    created_at      TEXT    NOT NULL,
    kind            TEXT    NOT NULL CHECK (kind IN ('mapping','line_forecast','published')),
    filename        TEXT,
    content_hash    TEXT    NOT NULL,
    payload_json    TEXT    NOT NULL,
    UNIQUE (dataset_id, input_version, kind, content_hash)
);

CREATE TABLE IF NOT EXISTS previews (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id      INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    input_version   INTEGER NOT NULL,
    created_at      TEXT    NOT NULL,
    request_json    TEXT    NOT NULL,
    result_json     TEXT    NOT NULL,
    superseded      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id      INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    version_number  INTEGER NOT NULL,
    created_at      TEXT    NOT NULL,
    input_version   INTEGER NOT NULL,
    parent_id       INTEGER REFERENCES snapshots(id),
    result_json     TEXT    NOT NULL,
    UNIQUE (dataset_id, version_number)
);

CREATE INDEX IF NOT EXISTS idx_revisions_dataset ON revisions(dataset_id, input_version);
CREATE INDEX IF NOT EXISTS idx_previews_dataset ON previews(dataset_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_dataset ON snapshots(dataset_id, version_number);

CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id      INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    created_at      TEXT    NOT NULL,
    kind            TEXT    NOT NULL,
    message         TEXT    NOT NULL,
    detail_json     TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_dataset ON events(dataset_id, id);

-- A delivery order is one CSV hand-off covering a set of periods.  Every
-- period is pinned to exactly one snapshot; ``latest_snapshot_version`` records
-- the newest published snapshot at the moment the order took effect.
CREATE TABLE IF NOT EXISTS delivery_orders (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_id              INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    created_at              TEXT    NOT NULL,
    latest_snapshot_version INTEGER NOT NULL,
    periods_json            TEXT    NOT NULL,
    new_periods_json        TEXT    NOT NULL,
    row_count               INTEGER NOT NULL,
    csv_text                TEXT    NOT NULL,
    -- Idempotency: a retried POST (same client key) returns the same order.
    request_key             TEXT
);
CREATE INDEX IF NOT EXISTS idx_delivery_orders_dataset ON delivery_orders(dataset_id, id);

CREATE TABLE IF NOT EXISTS delivery_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        INTEGER NOT NULL REFERENCES delivery_orders(id) ON DELETE CASCADE,
    period          TEXT    NOT NULL,
    snapshot_id     INTEGER NOT NULL REFERENCES snapshots(id),
    snapshot_version INTEGER NOT NULL,
    locked_now      INTEGER NOT NULL DEFAULT 0,
    row_count       INTEGER NOT NULL,
    UNIQUE (order_id, period)
);
CREATE INDEX IF NOT EXISTS idx_delivery_items_order ON delivery_items(order_id);

-- First delivery wins.  The primary key is what makes the pin survive
-- re-publishes, restarts and concurrent delivery orders.
CREATE TABLE IF NOT EXISTS delivery_locks (
    dataset_id      INTEGER NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    period          TEXT    NOT NULL,
    snapshot_id     INTEGER NOT NULL REFERENCES snapshots(id),
    snapshot_version INTEGER NOT NULL,
    order_id        INTEGER NOT NULL REFERENCES delivery_orders(id),
    created_at      TEXT    NOT NULL,
    PRIMARY KEY (dataset_id, period)
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass
class Dataset:
    id: int
    name: str
    created_at: str
    input_version: int


class Database:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._init()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(SCHEMA)
            self._migrate(conn)
        finally:
            conn.close()

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        """Additive migrations for databases created by an older build."""
        columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(delivery_orders)").fetchall()
        }
        if "request_key" not in columns:
            conn.execute("ALTER TABLE delivery_orders ADD COLUMN request_key TEXT")
        # Created after the column is guaranteed to exist, so databases from an
        # older build (which lack ``request_key``) migrate cleanly.
        conn.execute(
            """CREATE UNIQUE INDEX IF NOT EXISTS idx_delivery_orders_key
               ON delivery_orders(dataset_id, request_key)
               WHERE request_key IS NOT NULL"""
        )

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """Yield a connection; commit on success, roll back on error."""
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                if conn.in_transaction:
                    conn.execute("COMMIT")
            except Exception:
                if conn.in_transaction:
                    conn.execute("ROLLBACK")
                raise
            finally:
                conn.close()

    # ------------------------------------------------------------------
    # Dataset helpers
    # ------------------------------------------------------------------
    def create_dataset(self, name: str) -> Dataset:
        with self.connect() as conn:
            cur = conn.execute(
                "INSERT INTO datasets (name, created_at, input_version) VALUES (?,?,?)",
                (name, utc_now(), 1),
            )
            dataset_id = int(cur.lastrowid)
        return Dataset(dataset_id, name, utc_now(), 1)

    def get_dataset(self, dataset_id: int, conn: sqlite3.Connection | None = None) -> Dataset | None:
        if conn is not None:
            row = conn.execute("SELECT * FROM datasets WHERE id=?", (dataset_id,)).fetchone()
        else:
            with self.connect() as own_conn:
                row = own_conn.execute(
                    "SELECT * FROM datasets WHERE id=?", (dataset_id,)
                ).fetchone()
        if row is None:
            return None
        return Dataset(row["id"], row["name"], row["created_at"], row["input_version"])

    def list_datasets(self) -> list[Dataset]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM datasets ORDER BY id DESC").fetchall()
        return [Dataset(r["id"], r["name"], r["created_at"], r["input_version"]) for r in rows]

    def bump_input_version(self, conn: sqlite3.Connection, dataset_id: int) -> int:
        conn.execute(
            "UPDATE datasets SET input_version = input_version + 1 WHERE id=?",
            (dataset_id,),
        )
        conn.execute(
            "UPDATE previews SET superseded=1 WHERE dataset_id=? AND superseded=0",
            (dataset_id,),
        )
        row = conn.execute(
            "SELECT input_version FROM datasets WHERE id=?", (dataset_id,)
        ).fetchone()
        return int(row["input_version"])

    # ------------------------------------------------------------------
    # Revisions (latest input of each kind within an input version)
    # ------------------------------------------------------------------
    def add_revision(
        self,
        conn: sqlite3.Connection,
        dataset_id: int,
        input_version: int,
        kind: str,
        filename: str | None,
        content_hash: str,
        payload: Any,
    ) -> int:
        cur = conn.execute(
            """INSERT OR IGNORE INTO revisions
               (dataset_id, input_version, created_at, kind, filename, content_hash, payload_json)
               VALUES (?,?,?,?,?,?,?)""",
            (
                dataset_id,
                input_version,
                utc_now(),
                kind,
                filename,
                content_hash,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
            ),
        )
        if cur.lastrowid:
            return int(cur.lastrowid)
        row = conn.execute(
            """SELECT id FROM revisions
               WHERE dataset_id=? AND input_version=? AND kind=? AND content_hash=?""",
            (dataset_id, input_version, kind, content_hash),
        ).fetchone()
        return int(row["id"])

    def latest_revisions(
        self, dataset_id: int, conn: sqlite3.Connection | None = None
    ) -> dict[str, dict[str, Any]]:
        """Return the newest revision of each kind for a dataset."""
        query = "SELECT * FROM revisions WHERE dataset_id=? ORDER BY id DESC"
        if conn is not None:
            rows = conn.execute(query, (dataset_id,)).fetchall()
        else:
            with self.connect() as own_conn:
                rows = own_conn.execute(query, (dataset_id,)).fetchall()
        latest: dict[str, dict[str, Any]] = {}
        for row in rows:
            if row["kind"] in latest:
                continue
            latest[row["kind"]] = {
                "id": row["id"],
                "input_version": row["input_version"],
                "created_at": row["created_at"],
                "filename": row["filename"],
                "content_hash": row["content_hash"],
                "payload": json.loads(row["payload_json"]),
            }
        return latest

    def log_event(
        self,
        conn: sqlite3.Connection,
        dataset_id: int,
        kind: str,
        message: str,
        detail: Any | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO events (dataset_id, created_at, kind, message, detail_json) VALUES (?,?,?,?,?)",
            (
                dataset_id,
                utc_now(),
                kind,
                message,
                None if detail is None else json.dumps(detail, ensure_ascii=False, default=str),
            ),
        )

    def list_events(self, dataset_id: int, limit: int = 50) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM events WHERE dataset_id=? ORDER BY id DESC LIMIT ?",
                (dataset_id, limit),
            ).fetchall()
        return [
            {
                "id": r["id"],
                "created_at": r["created_at"],
                "kind": r["kind"],
                "message": r["message"],
                "detail": json.loads(r["detail_json"]) if r["detail_json"] else None,
            }
            for r in rows
        ]

    # ------------------------------------------------------------------
    # Previews
    # ------------------------------------------------------------------
    def create_preview(
        self,
        conn: sqlite3.Connection,
        dataset_id: int,
        input_version: int,
        request: Any,
        result: Any,
    ) -> int:
        cur = conn.execute(
            """INSERT INTO previews (dataset_id, input_version, created_at, request_json, result_json)
               VALUES (?,?,?,?,?)""",
            (
                dataset_id,
                input_version,
                utc_now(),
                json.dumps(request, ensure_ascii=False, sort_keys=True),
                json.dumps(result, ensure_ascii=False, sort_keys=True),
            ),
        )
        return int(cur.lastrowid)

    def get_preview(self, preview_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM previews WHERE id=?", (preview_id,)).fetchone()
            if row is None:
                return None
            # Is the input still the newest version for this dataset?
            ds = conn.execute(
                "SELECT input_version FROM datasets WHERE id=?", (row["dataset_id"],)
            ).fetchone()
        return {
            "id": row["id"],
            "dataset_id": row["dataset_id"],
            "input_version": row["input_version"],
            "created_at": row["created_at"],
            "superseded": bool(row["superseded"]),
            "request": json.loads(row["request_json"]),
            "result": json.loads(row["result_json"]),
            "current_input_version": ds["input_version"] if ds else None,
        }

    def latest_preview(self, dataset_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT id FROM previews WHERE dataset_id=? ORDER BY id DESC LIMIT 1",
                (dataset_id,),
            ).fetchone()
        return self.get_preview(row["id"]) if row else None

    # ------------------------------------------------------------------
    # Snapshots
    # ------------------------------------------------------------------
    def publish(
        self,
        conn: sqlite3.Connection,
        dataset_id: int,
        input_version: int,
        result: Any,
    ) -> dict[str, Any]:
        row = conn.execute(
            "SELECT COALESCE(MAX(version_number), 0) AS v FROM snapshots WHERE dataset_id=?",
            (dataset_id,),
        ).fetchone()
        version_number = int(row["v"]) + 1
        parent = conn.execute(
            "SELECT id FROM snapshots WHERE dataset_id=? ORDER BY version_number DESC LIMIT 1",
            (dataset_id,),
        ).fetchone()
        cur = conn.execute(
            """INSERT INTO snapshots
               (dataset_id, version_number, created_at, input_version, parent_id, result_json)
               VALUES (?,?,?,?,?,?)""",
            (
                dataset_id,
                version_number,
                utc_now(),
                input_version,
                parent["id"] if parent else None,
                json.dumps(result, ensure_ascii=False, sort_keys=True),
            ),
        )
        return {"id": int(cur.lastrowid), "version_number": version_number}

    def list_snapshots(self, dataset_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT id, version_number, created_at, input_version, parent_id
                   FROM snapshots WHERE dataset_id=? ORDER BY version_number DESC""",
                (dataset_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    def get_snapshot(
        self,
        dataset_id: int,
        version_number: int | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> dict[str, Any] | None:
        """Load a snapshot; pass ``conn`` to read inside an open transaction."""
        if conn is not None:
            row = self._snapshot_row(conn, dataset_id, version_number)
        else:
            with self.connect() as own_conn:
                row = self._snapshot_row(own_conn, dataset_id, version_number)
        if row is None:
            return None
        return {
            "id": row["id"],
            "dataset_id": row["dataset_id"],
            "version_number": row["version_number"],
            "created_at": row["created_at"],
            "input_version": row["input_version"],
            "parent_id": row["parent_id"],
            "result": json.loads(row["result_json"]),
        }

    @staticmethod
    def _snapshot_row(
        conn: sqlite3.Connection, dataset_id: int, version_number: int | None
    ) -> sqlite3.Row | None:
        if version_number is None:
            return conn.execute(
                """SELECT * FROM snapshots WHERE dataset_id=?
                   ORDER BY version_number DESC LIMIT 1""",
                (dataset_id,),
            ).fetchone()
        return conn.execute(
            "SELECT * FROM snapshots WHERE dataset_id=? AND version_number=?",
            (dataset_id, version_number),
        ).fetchone()

    # ------------------------------------------------------------------
    # Delivery orders and period -> snapshot locks
    # ------------------------------------------------------------------
    def latest_snapshot_version(self, conn: sqlite3.Connection, dataset_id: int) -> int | None:
        row = conn.execute(
            "SELECT MAX(version_number) AS v FROM snapshots WHERE dataset_id=?",
            (dataset_id,),
        ).fetchone()
        return None if row is None or row["v"] is None else int(row["v"])

    def get_delivery_locks(
        self, conn: sqlite3.Connection, dataset_id: int
    ) -> dict[str, dict[str, Any]]:
        rows = conn.execute(
            "SELECT * FROM delivery_locks WHERE dataset_id=?", (dataset_id,)
        ).fetchall()
        return {
            r["period"]: {
                "period": r["period"],
                "snapshot_id": r["snapshot_id"],
                "snapshot_version": r["snapshot_version"],
                "order_id": r["order_id"],
                "created_at": r["created_at"],
            }
            for r in rows
        }

    def create_delivery_order(
        self,
        conn: sqlite3.Connection,
        dataset_id: int,
        latest_snapshot_version: int,
        periods: list[str],
        new_periods: list[str],
        row_count: int,
        csv_text: str,
        items: list[dict[str, Any]],
        locks: list[dict[str, Any]],
        request_key: str | None = None,
    ) -> int:
        """Persist an order, its per-period items and any new period locks.

        The caller has already resolved every period to a snapshot inside this
        transaction, so a concurrent order either committed first (its locks are
        visible here) or will observe ours.
        """
        cur = conn.execute(
            """INSERT INTO delivery_orders
               (dataset_id, created_at, latest_snapshot_version, periods_json,
                new_periods_json, row_count, csv_text, request_key)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                dataset_id,
                utc_now(),
                latest_snapshot_version,
                json.dumps(periods, ensure_ascii=False),
                json.dumps(new_periods, ensure_ascii=False),
                row_count,
                csv_text,
                request_key,
            ),
        )
        order_id = int(cur.lastrowid)
        for item in items:
            conn.execute(
                """INSERT INTO delivery_items
                   (order_id, period, snapshot_id, snapshot_version, locked_now, row_count)
                   VALUES (?,?,?,?,?,?)""",
                (
                    order_id,
                    item["period"],
                    item["snapshot_id"],
                    item["snapshot_version"],
                    1 if item["locked_now"] else 0,
                    item["row_count"],
                ),
            )
        for lock in locks:
            conn.execute(
                """INSERT INTO delivery_locks
                   (dataset_id, period, snapshot_id, snapshot_version, order_id, created_at)
                   VALUES (?,?,?,?,?,?)""",
                (
                    dataset_id,
                    lock["period"],
                    lock["snapshot_id"],
                    lock["snapshot_version"],
                    order_id,
                    utc_now(),
                ),
            )
        return order_id

    def delivery_locks(self, dataset_id: int) -> dict[str, dict[str, Any]]:
        with self.connect() as conn:
            return self.get_delivery_locks(conn, dataset_id)

    def list_delivery_orders(self, dataset_id: int, limit: int = 30) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """SELECT * FROM delivery_orders WHERE dataset_id=?
                   ORDER BY id DESC LIMIT ?""",
                (dataset_id, limit),
            ).fetchall()
            items: dict[int, list[dict[str, Any]]] = {}
            if rows:
                placeholders = ",".join("?" for _ in rows)
                for item in conn.execute(
                    f"""SELECT * FROM delivery_items
                        WHERE order_id IN ({placeholders}) ORDER BY period""",
                    tuple(r["id"] for r in rows),
                ).fetchall():
                    items.setdefault(item["order_id"], []).append(dict(item))
        return [
            {
                "id": r["id"],
                "created_at": r["created_at"],
                "latest_snapshot_version": r["latest_snapshot_version"],
                "periods": json.loads(r["periods_json"]),
                "new_periods": json.loads(r["new_periods_json"]),
                "row_count": r["row_count"],
                "items": items.get(r["id"], []),
            }
            for r in rows
        ]

    def get_delivery_order(self, dataset_id: int, order_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM delivery_orders WHERE id=? AND dataset_id=?",
                (order_id, dataset_id),
            ).fetchone()
            if row is None:
                return None
            items = conn.execute(
                "SELECT * FROM delivery_items WHERE order_id=? ORDER BY period",
                (order_id,),
            ).fetchall()
        return {
            "id": row["id"],
            "created_at": row["created_at"],
            "latest_snapshot_version": row["latest_snapshot_version"],
            "periods": json.loads(row["periods_json"]),
            "new_periods": json.loads(row["new_periods_json"]),
            "row_count": row["row_count"],
            "csv_text": row["csv_text"],
            "items": [dict(i) for i in items],
        }
