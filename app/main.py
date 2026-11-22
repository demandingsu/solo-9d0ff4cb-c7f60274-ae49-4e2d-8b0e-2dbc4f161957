"""FastAPI application and HTTP routes."""

from __future__ import annotations

import os
import secrets
from pathlib import Path
from typing import Any
from urllib.parse import quote

from fastapi import FastAPI, Form, HTTPException, Request, UploadFile
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    Response,
)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import delivery as delivery_module
from .db import Database
from .errors import MissingPeriodsError, PublishError, ReconciliationError
from .service import (
    KIND_LABELS,
    ReconciliationService,
    adjustments_to_csv,
    conflicts_to_csv,
    result_to_csv,
)
from .reconcile import ReconciliationResult

BASE_DIR = Path(__file__).resolve().parent
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_UPLOAD_BYTES", 10 * 1024 * 1024))


def create_app(db_path: str | None = None) -> FastAPI:
    app = FastAPI(title="制造计划时序数据校正", version="1.0.0")
    database = Database(db_path or os.environ.get("DATABASE_PATH", "/data/app.db"))
    service = ReconciliationService(database)

    templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    def available_periods(dataset_id: int) -> list[str]:
        """Periods the scheduler can pick: latest snapshot plus pinned ones."""
        from .reconcile import _period_sort_key

        latest = database.get_snapshot(dataset_id)
        periods: set[str] = set()
        if latest is not None:
            periods.update(
                p.period
                for p in ReconciliationResult.from_dict(latest["result"]).periods
            )
        snapshots: dict[int, Any] = {}
        for lock in database.delivery_locks(dataset_id).values():
            version = lock["snapshot_version"]
            if version == (latest or {}).get("version_number"):
                continue
            if version not in snapshots:
                snapshots[version] = database.get_snapshot(dataset_id, version)
            snapshot = snapshots[version]
            if snapshot is not None:
                periods.update(
                    p.period
                    for p in ReconciliationResult.from_dict(snapshot["result"]).periods
                )
        return sorted(periods, key=_period_sort_key)

    def state_for(request: Request, dataset_id: int, **extra: Any) -> dict[str, Any]:
        state = service.dataset_state(dataset_id)
        state.update(
            {
                "request": request,
                "kind_labels": KIND_LABELS,
                "deliveries": database.list_delivery_orders(dataset_id),
                "delivery_locks": database.delivery_locks(dataset_id),
                "available_periods": available_periods(dataset_id),
                # One key per rendered form: a double-submit replays the same
                # order instead of creating a second one.
                "delivery_request_key": secrets.token_hex(12),
                **extra,
            }
        )
        return state

    @app.exception_handler(ReconciliationError)
    async def handle_reconciliation_error(request: Request, exc: ReconciliationError):
        wants_json = request.url.path.startswith("/api/")
        if wants_json:
            content: dict[str, Any] = {"error": str(exc), "code": exc.code}
            if isinstance(exc, MissingPeriodsError):
                content["missing"] = exc.missing
            return JSONResponse(status_code=400, content=content)
        dataset_id = request.path_params.get("dataset_id")
        if dataset_id is None or database.get_dataset(int(dataset_id)) is None:
            return templates.TemplateResponse(
                request,
                "error.html",
                {"message": str(exc), "code": exc.code, "title": "操作失败"},
                status_code=400,
            )
        extra: dict[str, Any] = {"error": str(exc), "error_code": exc.code}
        if isinstance(exc, MissingPeriodsError):
            extra["delivery_attempt"] = {"missing": exc.missing}
        return templates.TemplateResponse(
            request,
            "dataset.html",
            state_for(request, int(dataset_id), **extra),
            status_code=400,
        )

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------
    @app.get("/", response_class=HTMLResponse)
    def index(request: Request):
        return templates.TemplateResponse(
            request,
            "index.html",
            {"request": request, "datasets": database.list_datasets()},
        )

    @app.post("/datasets")
    def create_dataset(name: str = Form(...)):
        dataset = service.create_dataset(name)
        return RedirectResponse(url=f"/datasets/{dataset.id}", status_code=303)

    @app.get("/datasets/{dataset_id}", response_class=HTMLResponse)
    def dataset_page(request: Request, dataset_id: int):
        if database.get_dataset(dataset_id) is None:
            raise HTTPException(status_code=404, detail="数据集不存在")
        return templates.TemplateResponse(
            request,
            "dataset.html",
            state_for(request, dataset_id),
        )

    @app.post("/datasets/{dataset_id}/upload")
    def upload(
        dataset_id: int,
        kind: str = Form(...),
        file: UploadFile = None,  # type: ignore[assignment]
    ):
        if file is None:
            raise HTTPException(status_code=400, detail="未选择文件")
        data = file.file.read(MAX_UPLOAD_BYTES + 1)
        if len(data) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="文件超出大小限制")
        outcome = service.upload(dataset_id, kind, file.filename or "upload.csv", data)
        message = (
            f"{KIND_LABELS[outcome.kind]}：{outcome.row_count} 行"
            + ("，已更新输入版本" if outcome.changed else "，内容未变化")
        )
        return RedirectResponse(
            url=f"/datasets/{dataset_id}?uploaded=1&message={quote(message)}",
            status_code=303,
        )

    @app.post("/datasets/{dataset_id}/preview")
    def run_preview(dataset_id: int):
        preview = service.preview(dataset_id)
        return RedirectResponse(
            url=f"/datasets/{dataset_id}?preview={preview['preview_id']}", status_code=303
        )

    @app.post("/datasets/{dataset_id}/publish")
    def publish(dataset_id: int, preview_id: int = Form(...)):
        published = service.publish(dataset_id, preview_id)
        return RedirectResponse(
            url=f"/datasets/{dataset_id}?published={published['version_number']}",
            status_code=303,
        )

    # ------------------------------------------------------------------
    # Delivery (scheduler CSV hand-off)
    # ------------------------------------------------------------------
    @app.post("/datasets/{dataset_id}/deliver/preview")
    def deliver_preview(
        request: Request,
        dataset_id: int,
        periods: str = Form(""),
        request_key: str = Form(""),
    ):
        if database.get_dataset(dataset_id) is None:
            raise HTTPException(status_code=404, detail="数据集不存在")
        parsed = delivery_module.parse_periods(periods)
        preview = delivery_module.resolve(database, dataset_id, parsed)
        return templates.TemplateResponse(
            request,
            "deliver.html",
            {
                "request": request,
                "dataset": database.get_dataset(dataset_id),
                "periods_text": periods,
                "delivery": preview.to_dict(),
                "available_periods": available_periods(dataset_id),
                "delivery_locks": database.delivery_locks(dataset_id),
                "delivery_request_key": request_key or secrets.token_hex(12),
                "error": None,
            },
        )

    @app.post("/datasets/{dataset_id}/deliver")
    def deliver(
        dataset_id: int,
        periods: str = Form(""),
        request_key: str = Form(""),
    ):
        if database.get_dataset(dataset_id) is None:
            raise HTTPException(status_code=404, detail="数据集不存在")
        parsed = delivery_module.parse_periods(periods)
        order = delivery_module.deliver(
            database, dataset_id, parsed, request_key=request_key.strip() or None
        )
        periods_q = quote(",".join(order["periods"]))
        return RedirectResponse(
            url=f"/datasets/{dataset_id}?delivered={order['order_id']}&periods={periods_q}",
            status_code=303,
        )

    @app.get("/datasets/{dataset_id}/deliveries/{order_id}/download")
    def download_delivery(dataset_id: int, order_id: int):
        order = database.get_delivery_order(dataset_id, order_id)
        if order is None:
            raise HTTPException(status_code=404, detail="交付单不存在")
        return Response(
            content="﻿" + order["csv_text"],
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="delivery-order-{order_id}-d{dataset_id}.csv"'
                )
            },
        )

    @app.get("/datasets/{dataset_id}/snapshots/{version_number}", response_class=HTMLResponse)
    def snapshot_page(request: Request, dataset_id: int, version_number: int):
        snapshot = database.get_snapshot(dataset_id, version_number)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="快照不存在")
        result = ReconciliationResult.from_dict(snapshot["result"])
        return templates.TemplateResponse(
            request,
            "snapshot.html",
            {
                "request": request,
                "dataset": database.get_dataset(dataset_id),
                "snapshot": snapshot,
                "result": result,
                "kind_labels": KIND_LABELS,
                "readonly": True,
            },
        )

    # ------------------------------------------------------------------
    # Downloads
    # ------------------------------------------------------------------
    def _resolve_result(dataset_id: int, version_number: int | None, preview_id: int | None):
        if version_number is not None:
            snapshot = database.get_snapshot(dataset_id, version_number)
            if snapshot is None:
                raise HTTPException(status_code=404, detail="快照不存在")
            return ReconciliationResult.from_dict(snapshot["result"]), f"snapshot-v{version_number}"
        if preview_id is not None:
            preview = database.get_preview(preview_id)
            if preview is None or preview["dataset_id"] != dataset_id:
                raise HTTPException(status_code=404, detail="预览不存在")
            return ReconciliationResult.from_dict(preview["result"]), f"preview-{preview_id}"
        raise HTTPException(status_code=400, detail="需要 version 或 preview 参数")

    @app.get("/datasets/{dataset_id}/download/{filename}")
    def download(
        dataset_id: int,
        filename: str,
        version: int | None = None,
        preview: int | None = None,
    ):
        result, stem = _resolve_result(dataset_id, version, preview)
        if filename == "snapshot.csv":
            body = result_to_csv(result)
        elif filename == "adjustments.csv":
            body = adjustments_to_csv(result)
        elif filename == "conflicts.csv":
            body = conflicts_to_csv(result)
        else:
            raise HTTPException(status_code=404, detail="未知下载类型")
        return Response(
            content="﻿" + body,
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": f'attachment; filename="reconciliation-{stem}-{filename}"'
            },
        )

    # ------------------------------------------------------------------
    # JSON API (used by tests and automation)
    # ------------------------------------------------------------------
    @app.get("/api/datasets")
    def api_datasets():
        return [
            {"id": d.id, "name": d.name, "input_version": d.input_version}
            for d in database.list_datasets()
        ]

    @app.post("/api/datasets")
    def api_create_dataset(payload: dict[str, str]):
        dataset = service.create_dataset(payload.get("name", ""))
        return {"id": dataset.id, "name": dataset.name, "input_version": dataset.input_version}

    @app.get("/api/datasets/{dataset_id}")
    def api_state(dataset_id: int):
        return service.dataset_state(dataset_id)

    @app.post("/api/datasets/{dataset_id}/upload")
    async def api_upload(dataset_id: int, kind: str, file: UploadFile):
        data = await file.read(MAX_UPLOAD_BYTES + 1)
        if len(data) > MAX_UPLOAD_BYTES:
            raise HTTPException(status_code=413, detail="文件超出大小限制")
        outcome = service.upload(dataset_id, kind, file.filename or "upload.csv", data)
        return {
            "kind": outcome.kind,
            "changed": outcome.changed,
            "input_version": outcome.input_version,
            "row_count": outcome.row_count,
        }

    @app.post("/api/datasets/{dataset_id}/preview")
    def api_preview(dataset_id: int):
        return service.preview(dataset_id)

    @app.post("/api/datasets/{dataset_id}/publish")
    def api_publish(dataset_id: int, payload: dict[str, int]):
        return service.publish(dataset_id, int(payload["preview_id"]))

    @app.get("/api/datasets/{dataset_id}/snapshots")
    def api_snapshots(dataset_id: int):
        return database.list_snapshots(dataset_id)

    @app.get("/api/datasets/{dataset_id}/snapshots/{version_number}")
    def api_snapshot(dataset_id: int, version_number: int):
        snapshot = database.get_snapshot(dataset_id, version_number)
        if snapshot is None:
            raise HTTPException(status_code=404, detail="快照不存在")
        return snapshot

    @app.post("/api/datasets/{dataset_id}/deliver/preview")
    def api_deliver_preview(dataset_id: int, payload: dict[str, Any]):
        if database.get_dataset(dataset_id) is None:
            raise PublishError("数据集不存在。")
        periods = delivery_module.parse_periods(payload.get("periods"))
        return delivery_module.resolve(database, dataset_id, periods).to_dict()

    @app.post("/api/datasets/{dataset_id}/deliver")
    def api_deliver(dataset_id: int, payload: dict[str, Any]):
        periods = delivery_module.parse_periods(payload.get("periods"))
        order = delivery_module.deliver(
            database,
            dataset_id,
            periods,
            note=str(payload.get("note", "")),
            request_key=str(payload.get("request_key") or "") or None,
        )
        order.pop("csv_text", None)
        return order

    @app.get("/api/datasets/{dataset_id}/deliveries")
    def api_deliveries(dataset_id: int):
        return database.list_delivery_orders(dataset_id)

    @app.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return app


# Run with: uvicorn app.main:create_app --factory
