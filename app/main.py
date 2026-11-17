"""FastAPI entry point: REST API (with Swagger UI) + in-process UDP DNS."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .config import settings
from .dns_server import dns_service
from .failover import prober, registry
from .models import (
    ProbeTargetOut,
    PublishResult,
    RecordOut,
    VersionOut,
    ZoneOut,
    ZonePayload,
)
from .storage import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("main")


def _zone_out() -> ZoneOut:
    serial, records, published_at = storage.get_active_version()
    return ZoneOut(
        zone=settings.dns_zone,
        serial=serial,
        records=[RecordOut(**r) for r in records],
        published_at=published_at,
    )


def _version_out() -> VersionOut:
    serial, records, published_at = storage.get_active_version()
    sync_state = storage.get_sync_state()
    return VersionOut(
        role=settings.role,
        zone=settings.dns_zone,
        serial=serial,
        record_count=len(records),
        published_at=published_at,
        last_sync_at=sync_state["last_sync_at"],
        last_sync_status=sync_state["last_sync_status"],
        last_sync_error=sync_state["last_sync_error"] or None,
        upstream_serial=sync_state["upstream_serial"],
        probe_targets=[
            ProbeTargetOut(**target) for target in registry.describe(serial)
        ],
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info(
        "starting role=%s zone=%s db=%s dns_udp=%s api=%s",
        settings.role,
        settings.dns_zone,
        settings.db_path,
        settings.dns_port,
        settings.api_port,
    )
    dns_service.start()
    if not settings.is_primary:
        from .sync import sync_loop

        await sync_loop.start()
    try:
        yield
    finally:
        if not settings.is_primary:
            from .sync import sync_loop

            await sync_loop.stop()
        dns_service.stop()


app = FastAPI(
    title="Primary/Backup DNS for fixed intranet zone",
    version="2.0.0",
    description=(
        "Primary accepts a complete zone of up to 8 A records, mixing "
        "plain A records and managed A records (one name, two IPv4 "
        "addresses, one TCP probe port). Each DNS server probes both "
        "addresses itself, fails over to the backup after two consecutive "
        "primary failures while the backup is reachable, and fails back "
        "after two consecutive primary successes. Both down or no probe "
        "result -> SERVFAIL. The backup pulls whole immutable versions "
        "over HTTP; every DNS answer and its probe state belong to one "
        "version."
    ),
    lifespan=lifespan,
)


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "role": settings.role, "zone": settings.dns_zone}


@app.get("/api/version", response_model=VersionOut, tags=["zone"])
def get_version() -> VersionOut:
    return _version_out()


@app.get("/api/zone", response_model=ZoneOut, tags=["zone"])
def get_zone() -> ZoneOut:
    return _zone_out()


class SwitchRequest(BaseModel):
    name: str = Field(..., examples=["svc.internal.local"])
    primary_ip: str = Field(
        ...,
        description="Primary address of the managed record at the "
        "current active version.",
        examples=["10.0.0.11"],
    )


# ---------------------------------------------------------------- primary
@app.post(
    "/api/zone",
    response_model=PublishResult,
    status_code=201,
    tags=["primary"],
)
def publish_zone(payload: ZonePayload) -> PublishResult:
    """Replace the whole zone. The serial increments only on success."""
    if not settings.is_primary:
        raise HTTPException(
            status_code=403,
            detail="zone publishing is only allowed on the primary",
        )
    records = payload.to_storage_records()
    serial, published_at = storage.next_publish(records)
    # Move probe state to the new version and request a fresh round. The
    # publish call returns without waiting for any probe.
    registry.observe(serial, records)
    prober.kick()
    log.info("published new version serial=%s records=%s",
             serial, len(records))
    return PublishResult(
        ok=True,
        serial=serial,
        record_count=len(records),
        published_at=published_at,
    )


# ----------------------------------------------------------------- backup
@app.post("/api/sync", tags=["backup"])
async def manual_sync() -> dict:
    """Pull the complete current zone from the primary once."""
    if settings.is_primary:
        raise HTTPException(
            status_code=403,
            detail="sync is only available on the backup",
        )
    from .sync import sync_loop

    outcome = await sync_loop.trigger()
    result = outcome.as_dict()
    result["zone"] = _zone_out().model_dump()
    return result


# ----------------------------------------------------------------- failover
@app.post("/api/failover/switch", tags=["failover"])
def force_switch(payload: SwitchRequest) -> dict:
    """Operator override: force one managed record to its backup address.

    This is an explicit out-of-band action (e.g. planned maintenance). It
    only affects the active version's state; automatic failback still
    requires two consecutive primary successes. A forced switch is
    refused when the active version has no such managed record.
    """
    serial, records, _ = storage.get_active_version()
    name = payload.name.strip().rstrip(".").lower()
    target = None
    for rec in records:
        if rec["name"] == name and rec.get("primary_ip") is not None:
            target = rec
            break
    if target is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"no managed record {name!r} in active version "
                f"serial={serial}"
            ),
        )
    if target["primary_ip"] != payload.primary_ip:
        raise HTTPException(
            status_code=409,
            detail=(
                f"primary address mismatch: active version has "
                f"{target['primary_ip']!r}, request has "
                f"{payload.primary_ip!r}"
            ),
        )
    changed = registry.force_backup(serial, name)
    if not changed:
        raise HTTPException(
            status_code=409,
            detail=f"record {name!r} is not managed at serial {serial}",
        )
    log.warning(
        "operator forced %s to backup address (serial=%s)", name, serial
    )
    return {
        "ok": True,
        "serial": serial,
        "name": name,
        "active": "backup",
        "answer": target["backup_ip"],
    }


@app.get("/api/probe", tags=["failover"])
def probe_status() -> dict:
    """Current per-address probe results and selections of this server."""
    serial, records, _ = storage.get_active_version()
    return {
        "role": settings.role,
        "serial": serial,
        "targets": registry.describe(serial),
    }
