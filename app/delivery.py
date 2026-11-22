"""Delivery orders: hand a set of periods to the production scheduler as CSV.

Business rules
--------------
* An order is **all or nothing**.  If any requested period cannot be served from
  the snapshot it is pinned to — or the CSV cannot be built — the whole order is
  rejected: no order row, no items, no new locks.
* A period that was already delivered keeps the snapshot of its **first**
  delivery.  The pin lives in ``delivery_locks`` (primary key
  ``dataset_id, period``), so it survives later publishes, server restarts and
  repeated orders.
* A period that has never been delivered uses the newest snapshot that exists
  *when the order takes effect* (the latest published version read at the start
  of the order transaction).  Not the newest snapshot ever: a snapshot published
  while the CSV is being assembled must not leak into this order.
* Two orders racing on the same new period are serialised by SQLite's
  ``BEGIN IMMEDIATE`` write transaction.  The first to commit inserts the lock;
  the second re-reads the locks inside its own transaction and reuses that
  snapshot, so both orders deliver the same numbers.
* ``request_key`` makes retries idempotent: the same key replays the stored
  order instead of producing a second one.
* The readable CSV is stored with the order, so re-downloading an old order
  reproduces exactly what was handed over even after new snapshots exist.

Only ``line`` nodes are listed, one row per period and line, which is what the
scheduler imports ("产线编号").  The value is the corrected line forecast, never
the uploaded base.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from typing import Any

from . import db as db_module
from .errors import (
    DeliveryError,
    MissingPeriodsError,
    PublishError,
    ReconciliationError,
    SnapshotValidationError,
)
from .reconcile import ReconciliationResult, _period_sort_key

CSV_HEADER = [
    "period",
    "line_id",
    "line_name",
    "corrected_value",
    "snapshot_version",
]

# Characters that would make a CSV formula execute in a spreadsheet are
# neutralised so a hostile node id cannot turn a download into an attack.
_FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


def sanitize_cell(value: str) -> str:
    if value and value.startswith(_FORMULA_PREFIXES):
        return "'" + value
    return value


@dataclass
class DeliveryPreview:
    """What ``/deliver/preview`` shows before the scheduler commits."""

    periods: list[str]
    latest_snapshot_version: int | None
    last_delivered_version: int | None
    pinned: dict[str, int]  # period -> snapshot version, for previously delivered
    new_periods: list[str]
    missing: list[dict[str, Any]]  # {period, snapshot_version, reason}
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "periods": self.periods,
            "latest_snapshot_version": self.latest_snapshot_version,
            "last_delivered_version": self.last_delivered_version,
            "pinned": dict(
                sorted(self.pinned.items(), key=lambda kv: _period_sort_key(kv[0]))
            ),
            "new_periods": self.new_periods,
            "missing": self.missing,
            "warnings": self.warnings,
        }


# ----------------------------------------------------------------------
# Input validation
# ----------------------------------------------------------------------
def parse_periods(raw: str | list[str] | None) -> list[str]:
    """Accept a list, or free text separated by commas/whitespace."""
    if raw is None:
        items: list[str] = []
    elif isinstance(raw, str):
        text = raw.replace("，", ",").replace("\n", ",").replace("\r", ",")
        items = [chunk for part in text.split(",") for chunk in part.split()]
    else:
        items = [str(item) for item in raw]
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in items:
        value = str(item).strip()
        if not value or value in seen:
            continue
        if len(value) > 40:
            raise DeliveryError(f"时段 {value[:20]}… 过长，无法识别。")
        seen.add(value)
        cleaned.append(value)
    if not cleaned:
        raise DeliveryError("请至少选择一个时段。")
    if len(cleaned) > 400:
        raise DeliveryError("单次交付最多 400 个时段，请分批交付。")
    return cleaned


def _csv_value(value: float) -> str:
    # Fixed precision keeps the file byte-stable for a given snapshot.
    return f"{value:.6f}"


def _snapshot_periods(snapshot: dict[str, Any]) -> dict[str, Any]:
    result = ReconciliationResult.from_dict(snapshot["result"])
    return {period.period: period for period in result.periods}


def _load_snapshot(
    database: db_module.Database,
    conn: Any,
    dataset_id: int,
    version: int,
    cache: dict[int, dict[str, Any] | None],
) -> dict[str, Any] | None:
    if version not in cache:
        cache[version] = database.get_snapshot(dataset_id, version, conn)
    return cache[version]


def _missing_reason(pinned: bool, version: int) -> str:
    return (
        f"首次交付快照 v{version} 中没有该时段"
        if pinned
        else f"最新发布快照 v{version} 中没有该时段"
    )


# ----------------------------------------------------------------------
# Read-only resolution (preview page/API)
# ----------------------------------------------------------------------
def resolve(
    database: db_module.Database,
    dataset_id: int,
    periods: list[str],
) -> DeliveryPreview:
    """Resolve every period to a snapshot using the delivery rules.

    Read-only: writes nothing.
    """
    with database.connect() as conn:
        if database.get_dataset(dataset_id, conn) is None:
            raise PublishError("数据集不存在。")
        latest = database.latest_snapshot_version(conn, dataset_id)
        locks = database.get_delivery_locks(conn, dataset_id)
        warnings: list[str] = []
        pinned: dict[str, int] = {}
        new_periods: list[str] = []
        missing: list[dict[str, Any]] = []

        if latest is None:
            missing = [
                {"period": p, "snapshot_version": None, "reason": "尚无已发布快照"}
                for p in periods
            ]
            return DeliveryPreview(
                periods, None, None, pinned, new_periods, missing, warnings
            )

        snapshot_cache: dict[int, dict[str, Any] | None] = {}
        period_cache: dict[int, dict[str, Any]] = {}
        for period in periods:
            was_pinned = period in locks
            version = locks[period]["snapshot_version"] if was_pinned else latest
            if not was_pinned:
                new_periods.append(period)
            snapshot = _load_snapshot(database, conn, dataset_id, version, snapshot_cache)
            if snapshot is None:  # pragma: no cover - FK makes this impossible
                missing.append(
                    {"period": period, "snapshot_version": version, "reason": "快照不存在"}
                )
                continue
            if version not in period_cache:
                period_cache[version] = _snapshot_periods(snapshot)
            if period not in period_cache[version]:
                missing.append(
                    {
                        "period": period,
                        "snapshot_version": version,
                        "reason": _missing_reason(was_pinned, version),
                    }
                )
            elif was_pinned:
                pinned[period] = version

        last_delivered = (
            max(item["snapshot_version"] for item in locks.values()) if locks else None
        )
        if periods and len(pinned) == len(periods):
            warnings.append(
                "所选时段均已交付，本次沿用各自首次交付的快照，不会新增锁定记录。"
            )
        if new_periods:
            warnings.append(
                f"{len(new_periods)} 个新时段将锁定到本单生效时的最新发布快照 v{latest}。"
            )
    return DeliveryPreview(
        periods, latest, last_delivered, pinned, new_periods, missing, warnings
    )


# ----------------------------------------------------------------------
# CSV row building
# ----------------------------------------------------------------------
def build_rows(
    snapshot: dict[str, Any],
    period: str,
    period_result: Any,
    version: int,
) -> list[list[str]]:
    hierarchy = snapshot["result"]["hierarchy"]
    nodes = hierarchy["nodes"]
    rows: list[list[str]] = []
    for line in hierarchy["by_level"]["line"]:
        corrected = period_result.corrected.get(line)
        if corrected is None:
            raise SnapshotValidationError(
                f"快照 v{version} 的时段 {period} 缺少产线 {line} 的校正值，整单退回。"
            )
        name = (nodes.get(line) or {}).get("name") or line
        rows.append(
            [
                sanitize_cell(period),
                sanitize_cell(line),
                sanitize_cell(str(name)),
                _csv_value(float(corrected)),
                str(version),
            ]
        )
    return rows


# ----------------------------------------------------------------------
# Order creation
# ----------------------------------------------------------------------
def deliver(
    database: db_module.Database,
    dataset_id: int,
    periods: list[str],
    *,
    note: str = "",
    request_key: str | None = None,
) -> dict[str, Any]:
    """Build and record one delivery order atomically.

    ``request_key`` makes the call idempotent: retrying the same key replays the
    already-created order instead of locking a second time.

    Raises ``MissingPeriodsError`` (carrying every missing period) or another
    ``ReconciliationError`` on failure.  All database work happens in a single
    ``BEGIN IMMEDIATE`` transaction, so a raised error leaves no order, no items
    and no locks behind.
    """
    order: dict[str, Any] | None = None
    try:
        with database.connect() as conn:
            if database.get_dataset(dataset_id, conn) is None:
                raise PublishError("数据集不存在。")

            previous = None
            if request_key:
                previous = conn.execute(
                    """SELECT * FROM delivery_orders
                       WHERE dataset_id=? AND request_key=?""",
                    (dataset_id, request_key),
                ).fetchone()
                if previous is not None and set(
                    json.loads(previous["periods_json"])
                ) != set(periods):
                    raise DeliveryError(
                        "该交付请求编号已用于另一组时段，请刷新页面后重新提交。"
                    )

            if previous is None:
                order = _create_order(
                    database, conn, dataset_id, periods, note, request_key
                )
            else:
                order = _order_from_row(conn, previous)
    except ReconciliationError as exc:
        if order is None:
            # ``connect()`` already rolled the transaction back.  Record the
            # rejection on a fresh connection: the audit trail survives, the
            # order/items/locks do not exist.
            _log_rejection(database, dataset_id, periods, exc)
        raise
    return order


def _order_from_row(conn: Any, row: Any) -> dict[str, Any]:
    items = conn.execute(
        "SELECT * FROM delivery_items WHERE order_id=? ORDER BY period", (row["id"],)
    ).fetchall()
    return {
        "order_id": int(row["id"]),
        "periods": json.loads(row["periods_json"]),
        "new_periods": json.loads(row["new_periods_json"]),
        "versions": {i["period"]: i["snapshot_version"] for i in items},
        "row_count": row["row_count"],
        "csv_text": row["csv_text"],
        "latest_snapshot_version": row["latest_snapshot_version"],
        "replayed": True,
    }


def _create_order(
    database: db_module.Database,
    conn: Any,
    dataset_id: int,
    periods: list[str],
    note: str,
    request_key: str | None,
) -> dict[str, Any]:
    """Resolve, build and persist one order inside ``conn``'s transaction."""
    # "Latest" as the order takes effect.  Read once, before any CSV work, so a
    # snapshot published during assembly cannot enter this order.
    latest = database.latest_snapshot_version(conn, dataset_id)
    if latest is None:
        raise DeliveryError("数据集还没有已发布快照，无法交付。")

    # Locks are read inside the write transaction: a racing order that committed
    # first is visible here; one still running waits for this transaction.
    locks = database.get_delivery_locks(conn, dataset_id)

    chosen: dict[str, int] = {}
    new_periods: list[str] = []
    missing: list[dict[str, Any]] = []
    snapshot_cache: dict[int, dict[str, Any] | None] = {}
    period_cache: dict[int, dict[str, Any]] = {}

    for period in periods:
        was_pinned = period in locks
        version = locks[period]["snapshot_version"] if was_pinned else latest
        if not was_pinned:
            new_periods.append(period)
        chosen[period] = version
        snapshot = _load_snapshot(database, conn, dataset_id, version, snapshot_cache)
        if snapshot is None:  # pragma: no cover - FK prevents this
            missing.append(
                {"period": period, "snapshot_version": version, "reason": "快照不存在"}
            )
            continue
        if version not in period_cache:
            period_cache[version] = _snapshot_periods(snapshot)
        if period not in period_cache[version]:
            missing.append(
                {
                    "period": period,
                    "snapshot_version": version,
                    "reason": _missing_reason(was_pinned, version),
                }
            )

    if missing:
        missing.sort(key=lambda m: _period_sort_key(m["period"]))
        labels = "、".join(
            f"{m['period']}（快照 v{m['snapshot_version']}）" for m in missing
        )
        raise MissingPeriodsError(
            "以下时段在应取的快照中不存在，整单未交付：" + labels, missing
        )

    # Build the CSV before any INSERT.  A structural problem aborts the order.
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(CSV_HEADER)
    items: list[dict[str, Any]] = []
    total_rows = 0
    for period in periods:
        version = chosen[period]
        snapshot = snapshot_cache[version]
        assert snapshot is not None  # guaranteed by the missing check above
        rows = build_rows(snapshot, period, period_cache[version][period], version)
        writer.writerows(rows)
        total_rows += len(rows)
        items.append(
            {
                "period": period,
                "snapshot_id": snapshot["id"],
                "snapshot_version": version,
                "locked_now": period in new_periods,
                "row_count": len(rows),
            }
        )

    # Lock exactly the new periods.  Plain INSERT (not upsert) is deliberate:
    # a silent overwrite of an existing pin would break the first-delivery rule.
    lock_rows = [
        {
            "period": period,
            "snapshot_id": snapshot_cache[chosen[period]]["id"],
            "snapshot_version": chosen[period],
        }
        for period in new_periods
    ]
    order_id = database.create_delivery_order(
        conn,
        dataset_id,
        latest,
        periods,
        new_periods,
        total_rows,
        buffer.getvalue(),
        items,
        lock_rows,
        request_key,
    )
    database.log_event(
        conn,
        dataset_id,
        "delivery",
        f"交付单 #{order_id}：{len(periods)} 个时段、{total_rows} 行；"
        f"新锁定 {len(new_periods)} 个时段"
        + (f"，生效快照 v{latest}" if new_periods else "，全部沿用首次交付快照"),
        {
            "order_id": order_id,
            "periods": periods,
            "new_periods": new_periods,
            "versions": {p: chosen[p] for p in periods},
            "note": note,
        },
    )
    return {
        "order_id": order_id,
        "periods": periods,
        "new_periods": new_periods,
        "versions": {p: chosen[p] for p in periods},
        "row_count": total_rows,
        "csv_text": buffer.getvalue(),
        "latest_snapshot_version": latest,
        "replayed": False,
    }


def _log_rejection(
    database: db_module.Database,
    dataset_id: int,
    periods: list[str],
    exc: ReconciliationError,
) -> None:
    """Best-effort audit trail for a rejected order (nothing else is written)."""
    try:
        with database.connect() as conn:
            database.log_event(
                conn,
                dataset_id,
                "delivery_rejected",
                f"交付被退回：{exc}",
                {
                    "error_code": exc.code,
                    "periods": periods,
                    "missing": getattr(exc, "missing", None),
                },
            )
    except Exception:  # pragma: no cover - logging must not mask the error
        pass
