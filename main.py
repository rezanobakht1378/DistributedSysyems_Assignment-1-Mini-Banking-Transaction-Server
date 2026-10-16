from __future__ import annotations

import asyncio
import logging
import os
import re
import time
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from logging.handlers import RotatingFileHandler
from threading import BoundedSemaphore, Lock
from typing import Any

import httpx
import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from models import MAX_BALANCE, BankStore, Operation
from requestForms import ReplicationRequest, TransactionRequest

load_dotenv()

_REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")


@dataclass(frozen=True)
class Settings:
    database_url: str
    max_clients: int
    role: str
    node_id: str
    peer_url: str | None
    primary_url: str | None
    heartbeat_interval: float
    failure_threshold: int
    log_file: str

    @classmethod
    def from_environment(cls) -> Settings:
        role = os.getenv("BANK_ROLE", "primary").strip().lower()
        peer_url = os.getenv("BANK_PEER_URL") or None
        primary_url = os.getenv("BANK_PRIMARY_URL") or None
        max_clients = int(os.getenv("BANK_MAX_CLIENTS", "100"))
        heartbeat_interval = float(os.getenv("BANK_HEARTBEAT_INTERVAL", "1"))
        failure_threshold = int(os.getenv("BANK_FAILURE_THRESHOLD", "3"))
        if role not in {"primary", "backup"}:
            raise ValueError("BANK_ROLE must be 'primary' or 'backup'.")
        if role == "backup" and not primary_url:
            raise ValueError("BANK_PRIMARY_URL is required when BANK_ROLE=backup.")
        if max_clients < 1 or heartbeat_interval <= 0 or failure_threshold < 1:
            raise ValueError("Client limit and failover timing settings must be positive.")
        database_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/banking_db")
        return cls(
            database_url=database_url,
            max_clients=max_clients,
            role=role,
            node_id=os.getenv("BANK_NODE_ID", "server-a"),
            peer_url=peer_url.rstrip("/") if peer_url else None,
            primary_url=primary_url.rstrip("/") if primary_url else None,
            heartbeat_interval=heartbeat_interval,
            failure_threshold=failure_threshold,
            log_file=os.getenv("BANK_LOG_FILE", "banking_server.log"),
        )


def _configure_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger("banking_server")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)sZ %(levelname)s %(message)s")
        formatter.converter = time.gmtime
        file_handler = RotatingFileHandler(log_file, maxBytes=50 * 1024 * 1024, backupCount=3, encoding="utf-8")
        stream_handler = logging.StreamHandler()
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
    return logger


def _validated_operation(
    action: str,
    account: str | None,
    amount: Decimal | None,
    target_account: str | None = None,
) -> Operation:
    if action not in {"balance", "deposit", "withdraw", "transfer"}:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": "unsupported_action", "message": f"Action {action!r} is not supported."})
    if not account:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": "missing_field", "message": "account is required."})
    if action == "balance":
        return Operation(action="balance", account=account)
    if amount is None:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": "missing_field", "message": f"amount is required for {action}."})
    if amount <= 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": "invalid_amount", "message": "amount must be greater than zero."})
    if amount > MAX_BALANCE or amount.as_tuple().exponent < -2:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": "invalid_amount", "message": "amount must be within range and have no more than two decimal places."})
    amount = amount.quantize(Decimal("0.01"))
    if action == "transfer" and not target_account:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail={"error": "missing_field","message": "target_account is required for a transfer."})
    return Operation(action, account, amount, target_account)


def create_app(settings: Settings | None = None) -> FastAPI:
    config = settings or Settings.from_environment()
    logger = _configure_logger(config.log_file)
    store = BankStore(config.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        store.initialize()
        monitor: asyncio.Task[None] | None = None
        if app.state.role == "backup":
            monitor = asyncio.create_task(_watch_primary(app))
        try:
            yield
        finally:
            if monitor is not None:
                monitor.cancel()
                try:
                    await monitor
                except asyncio.CancelledError:
                    pass

    app = FastAPI(title="Reza Nobakht`s Mini Banking Transaction Server", lifespan=lifespan)
    app.state.settings = config
    app.state.store = store
    app.state.role = config.role
    app.state.peer_url = config.peer_url
    app.state.write_locked = False
    app.state.write_lock = Lock()
    app.state.logger = logger
    request_slots = BoundedSemaphore(config.max_clients)

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        supplied_id = request.headers.get("X-Request-ID", "")
        request_id = supplied_id if _REQUEST_ID_PATTERN.fullmatch(supplied_id) else uuid.uuid4().hex
        request.state.request_id = request_id
        request.state.account_id = request.query_params.get("account", "N/A")
        client_ip = request.client.host if request.client else "unknown"
        started = time.perf_counter()
        acquired = request_slots.acquire(blocking=False)
        try:
            if acquired:
                try:
                    response = await call_next(request)
                except Exception:
                    result = "error"
                    raise
                result = "success" if response.status_code < 400 else f"http_{response.status_code}"
            else:
                response = JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"status": "error", "error": "server_busy", "message": "Maximum concurrent request capacity is reached."})
                result = "http_503"
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            if acquired:
                request_slots.release()
            latency_ms = (time.perf_counter() - started) * 1000
            logger.info(
                "request_id=%r client_ip=%r action=%r account=%r result=%r latency_ms=%.2f",
                request_id,
                client_ip,
                request.method + " " + request.url.path,
                getattr(request.state, "account_id", "N/A"),
                result,
                latency_ms,
            )

    @app.exception_handler(HTTPException)
    async def http_error_handler(request: Request, error: HTTPException):
        if isinstance(error.detail, dict):
            content = {"status": "error", **error.detail}
        else:
            content = {"status": "error", "error": "request_error", "message": str(error.detail)}
        response = JSONResponse(status_code=error.status_code, content=content)
        request_id = getattr(request.state, "request_id", None)
        if request_id:
            response.headers["X-Request-ID"] = request_id
        return response

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, error: RequestValidationError):
        if isinstance(error.body, dict):
            request.state.account_id = error.body.get("account", "N/A")
        errors = [
            {
                "field": ".".join(str(part) for part in item["loc"][1:]),
                "message": item["msg"],
            }
            for item in error.errors()
        ]
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "status": "error",
                "error": "invalid_request",
                "message": "The request is malformed or contains invalid fields.",
                "details": errors,
            },
        )

    def execute_operation(request: Request, operation: Operation) -> dict[str, Any]:
        request.state.account_id = operation.account
        if operation.action == "balance":
            return store.get_balance(operation.account)

        if request.app.state.role != "primary":
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail={"error": "standby", "message": "This server is not currently accepting writes."})
        if request.app.state.write_locked:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "replication_uncertain",
                    "message": "Writes are paused because the replica could not be confirmed. Use the active backup.",
                },
            )

        request_id = request.state.request_id
        client_ip = request.client.host if request.client else "unknown"
        with request.app.state.write_lock:
            replica_response: dict[str, Any] | None = None
            peer_url = request.app.state.peer_url
            if peer_url:
                try:
                    response = httpx.post(
                        peer_url + "/internal/replicate",
                        json={
                            "action": operation.action,
                            "account": operation.account,
                            "amount": str(operation.amount),
                            "target_account": operation.target_account,
                            "request_id": request_id,
                            "client_ip": client_ip
                        },

                        timeout=3
                    )
                except httpx.RequestError as error:
                    request.app.state.write_locked = True
                    logger.error("Replica unavailable; writes locked: %s", error)
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "replica_unavailable",
                            "message": "The replica did not confirm the operation; writes are paused for safety.",
                        },
                    ) from error

                if response.status_code >= 500:
                    request.app.state.write_locked = True
                    logger.error("Replica returned HTTP %s; writes locked.", response.status_code)
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "replication_uncertain",
                            "message": "The replica could not confirm the operation; writes are paused for safety.",
                        },
                    )
                if response.status_code >= 400:
                    try:
                        body = response.json()
                    except ValueError as error:
                        raise HTTPException(
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail={
                                "error": "replication_rejected",
                                "message": "The replica returned an invalid response.",
                            },
                        ) from error
                    if not isinstance(body, dict):
                        raise HTTPException(
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail={
                                "error": "replication_rejected",
                                "message": "The replica returned an invalid response.",
                            },
                        )
                    if response.status_code == 409 and body.get("error") == "not_standby":
                        request.app.state.role = "standby"
                        request.app.state.peer_url = None
                        raise HTTPException(
                            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail={
                                "error": "role_changed",
                                "message": "This server has been superseded by the active backup.",
                            },
                        )
                    raise HTTPException(
                        status_code=response.status_code,
                        detail={
                            "error": body.get("error", "replication_rejected"),
                            "message": body.get("message", "The replica rejected the operation."),
                        },
                    )
                try:
                    replica_response = response.json()
                except ValueError as error:
                    request.app.state.write_locked = True
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "replication_uncertain",
                            "message": "The replica response could not be verified; writes are paused.",
                        },
                    ) from error

            try:
                local_response = store.execute(operation, request_id, client_ip)
            except Exception as error:
                if replica_response is not None:
                    request.app.state.write_locked = True
                    logger.exception("Replica committed but local commit failed; writes locked.")
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "replication_uncertain",
                            "message": "The operation reached the replica but local state could not be confirmed.",
                        },
                    ) from error
                raise

            if replica_response is not None and replica_response != local_response:
                request.app.state.write_locked = True
                logger.error("Primary and replica responses differ; writes locked.")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "replica_divergence",
                        "message": "The replica and primary disagree; writes are paused for safety.",
                    },
                )
            return local_response

    @app.get("/health")
    def health(request: Request):
        if request.app.state.write_locked:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "status": "unavailable",
                    "role": request.app.state.role,
                    "node_id": config.node_id,
                },
            )
        return {
            "status": "success",
            "role": request.app.state.role,
            "node_id": config.node_id,
        }

    @app.get("/balance/inquiry")
    def read_balance(
        request: Request,
        account: str,
    ):
        operation = _validated_operation("balance", account, None)
        return execute_operation(request, operation)

    @app.post("/transactions")
    def transact(body: TransactionRequest, request: Request):
        request.state.account_id = body.account or "N/A"
        operation = _validated_operation(
            body.action,
            body.account,
            body.amount,
            body.target_account,
        )
        return execute_operation(request, operation)

    @app.post("/internal/replicate")
    def replicate(body: ReplicationRequest, request: Request):
        request.state.account_id = body.account
        if request.app.state.role != "backup":
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={"error": "not_standby", "message": "This server is not a standby replica."})
        operation = Operation(
            action=body.action,
            account=body.account,
            amount=body.amount,
            target_account=body.target_account,
        )
        return store.execute(operation, body.request_id, body.client_ip)

    return app


async def _watch_primary(app: FastAPI) -> None:
    failures = 0
    config: Settings = app.state.settings
    assert config.primary_url is not None
    while app.state.role == "backup":
        try:
            async with httpx.AsyncClient(timeout=config.heartbeat_interval) as client:
                response = await client.get(config.primary_url + "/health")
            if response.status_code == 200:
                failures = 0
            else:
                failures += 1
        except httpx.RequestError:
            failures += 1

        if failures >= config.failure_threshold:
            app.state.role = "primary"
            app.state.peer_url = None
            app.state.logger.warning(
                "Primary %s unavailable; promoting node %s.",
                config.primary_url,
                config.node_id,
            )
            return
        await asyncio.sleep(config.heartbeat_interval)


settings = Settings.from_environment()
app = create_app(settings)


if __name__ == "__main__":
    uvicorn.run(app, host=os.getenv("BANK_HOST", "0.0.0.0"), port=int(os.getenv("BANK_PORT", "8000")))
