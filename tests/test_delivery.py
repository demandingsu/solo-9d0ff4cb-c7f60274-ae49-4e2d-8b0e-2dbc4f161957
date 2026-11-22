"""Delivery orders: first-delivery snapshot locking, all-or-nothing refunds."""

from __future__ import annotations

import csv
import io
import json
import threading

import pytest

from app import delivery as delivery_module
from app.db import Database
from app.delivery import deliver, parse_periods, resolve
from app.errors import DeliveryError, MissingPeriodsError, SnapshotValidationError
from app.service import ReconciliationService

from .conftest import FORECAST_CSV, MAPPING_CSV, upload_text

PUBLISHED_CSV = """node_id,level,period,value
,shop,2026-01,66
"""


def setup_dataset(tmp_path):
    db = Database(tmp_path / "deliver.db")
    svc = ReconciliationService(db)
    ds = svc.create_dataset("交付测试").id
    upload_text(svc, ds, "mapping", MAPPING_CSV)
    upload_text(svc, ds, "line_forecast", FORECAST_CSV)
    upload_text(svc, ds, "published", PUBLISHED_CSV)
    return db, svc, ds


def publish(svc, ds, published: str | None = None, forecast: str | None = None) -> int:
    if published is not None:
        upload_text(svc, ds, "published", published)
    if forecast is not None:
        upload_text(svc, ds, "line_forecast", forecast)
    preview = svc.preview(ds)
    return svc.publish(ds, preview["preview_id"])["version_number"]


def parse_csv(text: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text)))


def test_csv_columns_and_corrected_values(tmp_path):
    db, svc, ds = setup_dataset(tmp_path)
    v1 = publish(svc, ds)
    order = deliver(db, ds, ["2026-01"])
    rows = parse_csv(order["csv_text"])
    assert list(rows[0]) == [
        "period",
        "line_id",
        "line_name",
        "corrected_value",
        "snapshot_version",
    ]
    # Three lines per period, all stamped with the locked snapshot.
    assert {r["line_id"] for r in rows} == {"L1", "L2", "L3"}
    assert {r["snapshot_version"] for r in rows} == {str(v1)}
    # Corrected values (not the uploaded base) are exported.
    period = next(
        p
        for p in db.get_snapshot(ds, v1)["result"]["periods"]
        if p["period"] == "2026-01"
    )
    for row in rows:
        assert float(row["corrected_value"]) == pytest.approx(
            period["corrected"][row["line_id"]]
        )
    # Two periods -> two blocks with the same snapshot.
    order2 = deliver(db, ds, ["2026-01", "2026-02"])
    rows2 = parse_csv(order2["csv_text"])
    assert len(rows2) == 6
    assert {r["snapshot_version"] for r in rows2} == {str(v1)}


def test_first_delivery_snapshot_survives_republication(tmp_path):
    db, svc, ds = setup_dataset(tmp_path)
    v1 = publish(svc, ds)
    first = deliver(db, ds, ["2026-01"])
    assert {r["snapshot_version"] for r in parse_csv(first["csv_text"])} == {str(v1)}

    # Change the inputs and publish v2.  The already-delivered period must keep
    # v1; a fresh period takes the newest snapshot at order time (v2).
    v2 = publish(
        svc,
        ds,
        published=PUBLISHED_CSV.replace("66", "80"),
        forecast=FORECAST_CSV.replace("L1,2026-02,12", "L1,2026-02,42"),
    )
    assert v2 == v1 + 1

    resend = deliver(db, ds, ["2026-01"])
    assert {r["snapshot_version"] for r in parse_csv(resend["csv_text"])} == {str(v1)}
    # Values are the v1 numbers, not the re-published ones.
    v1_rows = {
        (r["period"], r["line_id"]): r["corrected_value"]
        for r in parse_csv(first["csv_text"])
    }
    v2_rows = {
        (r["period"], r["line_id"]): r["corrected_value"]
        for r in parse_csv(resend["csv_text"])
    }
    assert v2_rows == v1_rows

    mixed = deliver(db, ds, ["2026-01", "2026-02"])
    versions = {r["period"]: r["snapshot_version"] for r in parse_csv(mixed["csv_text"])}
    assert versions == {"2026-01": str(v1), "2026-02": str(v2)}
    assert mixed["new_periods"] == ["2026-02"]
    # Only the new period earned a lock record.
    locks = db.delivery_locks(ds)
    assert set(locks) == {"2026-01", "2026-02"}
    assert locks["2026-01"]["order_id"] == first["order_id"]
    assert locks["2026-02"]["order_id"] == mixed["order_id"]


def test_download_reproduces_original_order_after_new_snapshot(tmp_path):
    db, svc, ds = setup_dataset(tmp_path)
    publish(svc, ds)
    order = deliver(db, ds, ["2026-01"])
    original = db.get_delivery_order(ds, order["order_id"])["csv_text"]
    publish(svc, ds, published=PUBLISHED_CSV.replace("66", "70"))
    again = db.get_delivery_order(ds, order["order_id"])["csv_text"]
    assert again == original


def test_missing_period_rejects_whole_order_and_writes_nothing(tmp_path):
    db, svc, ds = setup_dataset(tmp_path)
    publish(svc, ds)
    before_orders = db.list_delivery_orders(ds)
    before_locks = db.delivery_locks(ds)
    before_snapshots = db.list_snapshots(ds)

    with pytest.raises(MissingPeriodsError) as excinfo:
        deliver(db, ds, ["2026-01", "2026-03"])
    assert [m["period"] for m in excinfo.value.missing] == ["2026-03"]
    assert excinfo.value.missing[0]["snapshot_version"] == 1

    assert db.list_delivery_orders(ds) == before_orders
    assert db.delivery_locks(ds) == before_locks
    assert db.list_snapshots(ds) == before_snapshots
    # The rejection is audited but nothing else changed.
    events = db.list_events(ds)
    assert any(e["kind"] == "delivery_rejected" for e in events)


def test_pinned_period_missing_from_first_snapshot_is_reported(tmp_path):
    """A period locked to v1 but absent there cannot be served, even if v2 has it."""
    db, svc, ds = setup_dataset(tmp_path)
    v1 = publish(svc, ds)
    deliver(db, ds, ["2026-01"])
    # v2 adds a period; v1 does not have it, but the lock for 2026-01 is v1.
    # Re-requesting 2026-01 plus a new period that only exists in v2 is fine.
    v2 = publish(
        svc,
        ds,
        forecast=FORECAST_CSV
        + "L1,2026-03,1\nL2,2026-03,2\nL3,2026-03,3\n",
    )
    ok = deliver(db, ds, ["2026-01", "2026-03"])
    assert {r["period"] for r in parse_csv(ok["csv_text"])} == {"2026-01", "2026-03"}

    # Now simulate the pinned snapshot losing the period (corrupted pin).
    v1_id = db.get_snapshot(ds, v1)["id"]
    with db.connect() as conn:
        conn.execute(
            "UPDATE delivery_locks SET snapshot_version=?, snapshot_id=? WHERE dataset_id=? AND period=?",
            (v1, v1_id, ds, "2026-03"),
        )
    with pytest.raises(MissingPeriodsError) as excinfo:
        deliver(db, ds, ["2026-03"])
    assert excinfo.value.missing[0]["period"] == "2026-03"
    assert "首次交付快照" in excinfo.value.missing[0]["reason"]
    assert v2 != v1


def test_preview_reports_pins_and_missing_without_writing(tmp_path):
    db, svc, ds = setup_dataset(tmp_path)
    publish(svc, ds)
    deliver(db, ds, ["2026-01"])
    preview = resolve(db, ds, ["2026-01", "2026-02", "2026-09"])
    data = preview.to_dict()
    assert data["pinned"] == {"2026-01": 1}
    assert data["new_periods"] == ["2026-02", "2026-09"]
    assert [m["period"] for m in data["missing"]] == ["2026-09"]
    # Read-only: nothing was locked or ordered by the preview.
    assert set(db.delivery_locks(ds)) == {"2026-01"}
    assert len(db.list_delivery_orders(ds)) == 1


def test_snapshot_published_while_delivering_does_not_change_the_pin(tmp_path):
    """A snapshot published during CSV assembly must not enter this order."""
    db, svc, ds = setup_dataset(tmp_path)
    v1 = publish(svc, ds)
    original_write = db.create_delivery_order
    published_version: dict[str, int] = {}

    def racing_write(conn, dataset_id, latest, periods, new_periods, *args, **kwargs):
        # Simulate a publish that lands after the order resolved its snapshot
        # but before it commits: the snapshot must be a brand-new version.
        snapshot_row = conn.execute(
            "SELECT COALESCE(MAX(version_number),0)+1 AS v FROM snapshots WHERE dataset_id=?",
            (dataset_id,),
        ).fetchone()
        v_next = int(snapshot_row["v"])
        v1_result = db.get_snapshot(ds, v1, conn)["result"]
        conn.execute(
            """INSERT INTO snapshots
               (dataset_id, version_number, created_at, input_version, parent_id, result_json)
               VALUES (?,?,?,?,?,?)""",
            (
                dataset_id,
                v_next,
                "2099-01-01T00:00:00+00:00",
                1,
                None,
                json.dumps(v1_result),
            ),
        )
        published_version["v"] = v_next
        return original_write(
            conn, dataset_id, latest, periods, new_periods, *args, **kwargs
        )

    db.create_delivery_order = racing_write  # type: ignore[method-assign]
    try:
        order = deliver(db, ds, ["2026-01"])
    finally:
        db.create_delivery_order = original_write  # type: ignore[method-assign]

    assert published_version["v"] == v1 + 1
    # The order used the version it resolved (v1), not the one published mid-way.
    assert order["latest_snapshot_version"] == v1
    assert {r["snapshot_version"] for r in parse_csv(order["csv_text"])} == {str(v1)}
    assert db.delivery_locks(ds)["2026-01"]["snapshot_version"] == v1


def test_two_orders_racing_for_the_same_new_period_lock_once(tmp_path):
    db, svc, ds = setup_dataset(tmp_path)
    v1 = publish(svc, ds)

    barrier = threading.Barrier(2)
    results: list[dict] = []
    errors: list[BaseException] = []

    def worker():
        try:
            barrier.wait(timeout=10)
            results.append(deliver(db, ds, ["2026-01"]))
        except BaseException as exc:  # pragma: no cover - reported below
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    assert not errors
    assert len(results) == 2
    # Both orders used the same snapshot and produced byte-identical CSVs.
    assert results[0]["csv_text"] == results[1]["csv_text"]
    assert all(r["versions"] == {"2026-01": v1} for r in results)
    locks = db.delivery_locks(ds)
    assert locks["2026-01"]["snapshot_version"] == v1
    # Exactly one of the two orders created the lock.
    locked_now = [
        item
        for order in db.list_delivery_orders(ds)
        for item in order["items"]
        if item["locked_now"]
    ]
    assert len(locked_now) == 1


def test_two_orders_racing_against_different_latest_versions_agree(tmp_path):
    """A publish between the two orders must not split them onto two versions."""
    db, svc, ds = setup_dataset(tmp_path)
    v1 = publish(svc, ds)
    first = deliver(db, ds, ["2026-01"])
    publish(svc, ds, published=PUBLISHED_CSV.replace("66", "80"))

    barrier = threading.Barrier(2)
    results: list[dict] = []
    errors: list[BaseException] = []

    def worker():
        try:
            barrier.wait(timeout=10)
            results.append(deliver(db, ds, ["2026-01"]))
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)

    assert not errors
    assert all(r["csv_text"] == first["csv_text"] for r in results)
    assert db.delivery_locks(ds)["2026-01"]["snapshot_version"] == v1


def test_parse_periods_accepts_commas_spaces_and_dedupes():
    assert parse_periods("2026-01, 2026-02 2026-01") == ["2026-01", "2026-02"]
    assert parse_periods([" 2026-02 ", "2026-01", "2026-02"]) == ["2026-02", "2026-01"]
    with pytest.raises(DeliveryError):
        parse_periods("   ")
    with pytest.raises(DeliveryError):
        parse_periods(",")


def test_csv_sanitises_formula_cells(tmp_path):
    db, svc, ds = setup_dataset(tmp_path)
    # Rename a line to a formula-looking id and verify the download neutralises it.
    mapping = MAPPING_CSV.replace("L3,S2,line,焊装一线,1", "=cmd,S2,line,焊装一线,1")
    upload_text(svc, ds, "mapping", mapping)
    forecast = (
        "node_id,period,value\nL1,2026-01,10\nL2,2026-01,20\n=cmd,2026-01,30\n"
    )
    upload_text(svc, ds, "line_forecast", forecast)
    upload_text(svc, ds, "published", PUBLISHED_CSV)
    v = publish(svc, ds)
    order = deliver(db, ds, ["2026-01"])
    rows = parse_csv(order["csv_text"])
    assert any(r["line_id"] == "'=cmd" for r in rows)
    assert {r["snapshot_version"] for r in rows} == {str(v)}


def test_delivering_without_snapshot_rejected(tmp_path):
    db, svc, ds = setup_dataset(tmp_path)
    with pytest.raises(DeliveryError):
        deliver(db, ds, ["2026-01"])
    assert db.delivery_locks(ds) == {}
    assert db.list_delivery_orders(ds) == []


def test_snapshot_result_missing_line_aborts_order(tmp_path, monkeypatch):
    db, svc, ds = setup_dataset(tmp_path)
    publish(svc, ds)
    original = delivery_module.build_rows

    def broken(snapshot, period, period_result, version):
        if period == "2026-02":
            raise SnapshotValidationError("制表失败")
        return original(snapshot, period, period_result, version)

    monkeypatch.setattr(delivery_module, "build_rows", broken)
    with pytest.raises(SnapshotValidationError):
        deliver(db, ds, ["2026-01", "2026-02"])
    assert db.delivery_locks(ds) == {}
    assert db.list_delivery_orders(ds) == []


def test_request_key_makes_retry_idempotent(tmp_path):
    db, svc, ds = setup_dataset(tmp_path)
    publish(svc, ds)
    first = deliver(db, ds, ["2026-01"], request_key="retry-1")
    replay = deliver(db, ds, ["2026-01"], request_key="retry-1")
    assert replay["order_id"] == first["order_id"]
    assert replay["replayed"] is True
    assert replay["csv_text"] == first["csv_text"]
    assert len(db.list_delivery_orders(ds)) == 1
    # Re-using a key for different periods is rejected, not silently replayed.
    with pytest.raises(DeliveryError, match="请求编号"):
        deliver(db, ds, ["2026-02"], request_key="retry-1")


def test_old_database_is_migrated_with_request_key(tmp_path):
    import sqlite3

    path = tmp_path / "old.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """CREATE TABLE delivery_orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dataset_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            latest_snapshot_version INTEGER NOT NULL,
            periods_json TEXT NOT NULL,
            new_periods_json TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            csv_text TEXT NOT NULL
        );"""
    )
    conn.commit()
    conn.close()
    # Opening the database runs the additive migration.
    Database(path)
    with sqlite3.connect(path) as check:
        columns = {row[1] for row in check.execute("PRAGMA table_info(delivery_orders)")}
    assert "request_key" in columns
