"""Pydantic models and zone validation rules shared by both roles.

A zone contains up to ``MAX_RECORDS`` A records in total. Two kinds of
records are accepted:

* a plain A record: ``{"name": "web.internal.local", "ip": "10.0.0.21"}``
* a managed A record: ``{"name": "svc.internal.local",
  "primary_ip": "10.0.0.11", "backup_ip": "10.0.0.12",
  "port": 5432}``

A managed record is one DNS name backed by two addresses. Both addresses
are probed over the same TCP port and DNS answers whichever address the
failover state machine currently selects. The two forms share the same
name space and the same 8-record zone limit.
"""
from __future__ import annotations

import ipaddress
import re
from typing import Any, Dict, List, Optional

from pydantic import (
    AliasChoices,
    BaseModel,
    Field,
    field_validator,
    model_validator,
)

from .config import settings

_LABEL = re.compile(r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$")


class RecordInput(BaseModel):
    """One record in a published zone.

    Either ``ip`` (plain A) or the ``primary_ip``/``backup_ip``/``port``
    triple (managed A) must be supplied. Supplying both forms in one
    record is rejected so a zone can never be ambiguous.
    """

    name: str = Field(..., examples=["db.internal.local"])
    ip: Optional[str] = Field(
        None,
        description="Plain A record address. Omit for a managed record.",
        examples=["10.0.0.11"],
    )
    primary_ip: Optional[str] = Field(
        None,
        validation_alias=AliasChoices(
            "primary_ip", "primary", "primary_address", "ip_primary"
        ),
        description="Managed record: primary IPv4 address.",
        examples=["10.0.0.11"],
    )
    backup_ip: Optional[str] = Field(
        None,
        validation_alias=AliasChoices(
            "backup_ip", "backup", "secondary_ip", "backup_address"
        ),
        description="Managed record: backup IPv4 address.",
        examples=["10.0.0.12"],
    )
    port: Optional[int] = Field(
        None,
        ge=1,
        le=65535,
        validation_alias=AliasChoices("port", "probe_port", "tcp_port"),
        description=(
            "Managed record: the same TCP port probed on both addresses."
        ),
        examples=[5432],
    )

    @field_validator("name")
    @classmethod
    def _strip_name(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError("name must be a string")
        return v.strip().rstrip(".").lower()

    @field_validator("ip", "primary_ip", "backup_ip")
    @classmethod
    def _valid_ip(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        try:
            ip = ipaddress.ip_address(v.strip())
        except ValueError as exc:
            raise ValueError(f"invalid IP address: {v}") from exc
        if ip.version != 4:
            raise ValueError("only IPv4 A records are supported")
        return str(ip)

    @model_validator(mode="after")
    def _validate_shape(self) -> "RecordInput":
        has_plain = self.ip is not None
        has_managed = any(
            value is not None
            for value in (self.primary_ip, self.backup_ip, self.port)
        )
        if has_plain and has_managed:
            raise ValueError(
                f"record {self.name!r} mixes a plain A record with a "
                "managed record"
            )
        if not has_plain:
            if not all(
                value is not None
                for value in (self.primary_ip, self.backup_ip, self.port)
            ):
                raise ValueError(
                    f"record {self.name!r} must be either a plain A record "
                    "(name+ip) or a managed record "
                    "(name+primary_ip+backup_ip+port)"
                )
            if self.primary_ip == self.backup_ip:
                raise ValueError(
                    f"record {self.name!r} must use two distinct addresses"
                )
        return self

    @property
    def managed(self) -> bool:
        return self.ip is None

    def to_storage(self) -> Dict[str, Any]:
        """Return the canonical dict persisted in a zone version."""
        if self.ip is not None:
            return {"name": self.name, "ip": self.ip}
        return {
            "name": self.name,
            "primary_ip": self.primary_ip,
            "backup_ip": self.backup_ip,
            "port": self.port,
        }


class ZonePayload(BaseModel):
    """A complete replacement of the fixed zone (max 8 A records total)."""

    records: List[RecordInput]

    @model_validator(mode="after")
    def _validate_zone(self) -> "ZonePayload":
        if not self.records:
            raise ValueError("zone must contain at least one A record")
        if len(self.records) > settings.max_records:
            raise ValueError(
                f"a zone may contain at most {settings.max_records} "
                "A records (plain and managed combined)"
            )
        zone = settings.dns_zone.rstrip(".").lower()
        suffix = "." + zone
        seen: set[str] = set()
        for rec in self.records:
            name = rec.name
            if name != zone and not name.endswith(suffix):
                raise ValueError(
                    f"record {name!r} is outside the fixed zone {zone!r}"
                )
            owner = name[: -len(suffix)] if name.endswith(suffix) else ""
            labels = owner.split(".") if owner else []
            if any(not _LABEL.match(label) for label in labels):
                raise ValueError(f"invalid owner name: {name!r}")
            if name in seen:
                raise ValueError(f"duplicate A record name: {name!r}")
            seen.add(name)
        return self

    def sorted_records(self) -> List[RecordInput]:
        return sorted(self.records, key=lambda r: r.name)

    def to_storage_records(self) -> List[Dict[str, Any]]:
        return [rec.to_storage() for rec in self.sorted_records()]


def normalise_zone_records(records: Any) -> List[Dict[str, Any]]:
    """Validate a complete zone from the API or a sync payload.

    Raises ``pydantic.ValidationError`` for any malformed record, mixed
    plain/managed fields, duplicate names, out-of-zone names or a total
    count above ``MAX_RECORDS``.
    """
    return ZonePayload.model_validate({"records": records}).to_storage_records()


class RecordOut(BaseModel):
    name: str
    ip: Optional[str] = None
    primary_ip: Optional[str] = None
    backup_ip: Optional[str] = None
    port: Optional[int] = None


class ZoneOut(BaseModel):
    zone: str
    serial: int
    records: List[RecordOut]
    published_at: Optional[str] = None


class ProbeAddressOut(BaseModel):
    latest_ok: Optional[bool] = None
    ok_streak: int = 0
    fail_streak: int = 0


class ProbeTargetOut(BaseModel):
    name: str
    primary_ip: str
    backup_ip: str
    port: int
    active: Optional[str] = None
    answer: Optional[str] = None
    primary: ProbeAddressOut
    backup: ProbeAddressOut


class VersionOut(BaseModel):
    role: str
    zone: str
    serial: int
    record_count: int
    published_at: Optional[str] = None
    last_sync_at: Optional[str] = None
    last_sync_status: str = "never"
    last_sync_error: Optional[str] = None
    upstream_serial: Optional[int] = None
    probe_targets: List[ProbeTargetOut] = Field(default_factory=list)


class PublishResult(BaseModel):
    ok: bool
    serial: int
    record_count: int
    published_at: str
