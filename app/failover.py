"""Managed A record health probing and the primary/backup failover state.

A managed A record binds one DNS name to two IPv4 addresses and one TCP
port. Each DNS server probes both addresses independently in the
background and answers only the currently selected address.

Failover rules (per DNS server, per managed record):

* primary is selected initially;
* switch primary -> backup only after the primary fails two consecutive
  probes *and* the backup is currently reachable;
* switch backup -> primary only after the primary succeeds two
  consecutive probes;
* while no probe result exists for the address that would be answered,
  or when both addresses are unreachable, the name has no usable answer
  and the DNS layer returns SERVFAIL.

Staying on the primary during a single failure: that address is still
answered because it is the currently *selected* address, even though its
last probe failed. This keeps one lost packet from flapping the name; the
second consecutive failure triggers the switch.

Version isolation: probe state is keyed by the zone version serial. A new
version starts with fresh counters and selection, and state belonging to
an older serial is discarded. A DNS reply therefore always derives its
address from probe results of the same version that supplied the record
snapshot; stale results from before a zone change can never influence a
new version. If the backup's sync is broken it keeps probing and
answering its current (old) version; once it syncs a new version, probing
follows the new targets.

The probe loop runs in its own daemon thread with a bounded worker pool
and short socket timeouts. It never holds the registry lock while
connecting and never touches the DNS request path, so slow or timed-out
probes cannot delay UDP replies, publishing or syncing.
"""
from __future__ import annotations

import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .config import settings

log = logging.getLogger("failover")


@dataclass
class AddressState:
    latest_ok: Optional[bool] = None
    ok_streak: int = 0
    fail_streak: int = 0


@dataclass
class ManagedState:
    serial: int
    name: str
    primary_ip: str
    backup_ip: str
    port: int
    selected: str = "primary"
    epoch: float = 0.0
    primary: AddressState = field(default_factory=AddressState)
    backup: AddressState = field(default_factory=AddressState)


@dataclass(frozen=True)
class ProbeTask:
    serial: int
    name: str
    side: str  # "primary" | "backup"
    ip: str
    port: int


def _fresh_states(
    serial: int, records: List[Dict[str, Any]]
) -> Dict[str, ManagedState]:
    states: Dict[str, ManagedState] = {}
    for rec in records:
        if rec.get("ip") is not None or "primary_ip" not in rec:
            continue  # plain A record: no probing
        states[rec["name"]] = ManagedState(
            serial=serial,
            name=rec["name"],
            primary_ip=rec["primary_ip"],
            backup_ip=rec["backup_ip"],
            port=int(rec["port"]),
        )
    return states


def tcp_probe(ip: str, port: int, timeout: float) -> bool:
    """One TCP reachability probe, bounded by ``timeout`` seconds."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((ip, port))
        return True
    except OSError:
        return False
    finally:
        try:
            sock.close()
        except OSError:  # pragma: no cover
            pass


@dataclass
class Decision:
    answer: Optional[str]
    active: Optional[str]


class FailoverRegistry:
    """Thread-safe in-memory probe state, isolated per version serial."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._serial: Optional[int] = None
        self._versions: Dict[int, Dict[str, ManagedState]] = {}
        self._in_flight: set[Tuple[int, str, str]] = set()

    # -- version handling ----------------------------------------------
    def observe(
        self, serial: int, records: List[Dict[str, Any]]
    ) -> List[ProbeTask]:
        """Sync registry with the active version; return probe tasks.

        Called by the prober every tick and cheaply by the DNS resolver
        (which only needs the state container to exist). Results from
        older versions are dropped the moment a newer serial is seen.
        """
        with self._lock:
            if self._serial is None or serial > self._serial:
                previous_serial = self._serial
                previous = (
                    self._versions.get(previous_serial)
                    if previous_serial is not None
                    else None
                )
                self._serial = serial
                # Fresh, empty counters: no result from an old version may
                # ever answer the new one.
                self._versions = {serial: _fresh_states(serial, records)}
                if previous is not None:
                    # Keep the immediately previous snapshot's state only,
                    # so a resolver that read the old snapshot just before
                    # the switch still answers consistently with that old
                    # version instead of mixing. It is never used for the
                    # new serial.
                    self._versions[previous_serial] = previous
                stale = [
                    key for key in self._in_flight
                    if key[0] not in self._versions
                ]
                for key in stale:
                    self._in_flight.discard(key)
            elif serial == self._serial and serial not in self._versions:
                self._versions[serial] = _fresh_states(serial, records)

            states = self._versions.get(serial)
            if states is None:
                return []
            tasks: List[ProbeTask] = []
            for state in states.values():
                tasks.append(
                    ProbeTask(
                        state.serial, state.name, "primary",
                        state.primary_ip, state.port,
                    )
                )
                tasks.append(
                    ProbeTask(
                        state.serial, state.name, "backup",
                        state.backup_ip, state.port,
                    )
                )
            return tasks

    def begin_probe(self, task: ProbeTask) -> bool:
        """Claim a probe slot; at most one in flight per address."""
        key = (task.serial, task.name, task.side)
        with self._lock:
            if key in self._in_flight:
                return False
            if task.serial not in self._versions:
                return False
            self._in_flight.add(key)
            return True

    def record_result(
        self, task: ProbeTask, ok: bool, not_before: float = 0.0
    ) -> None:
        """Apply one finished probe to the state of its own version only.

        ``not_before`` is the monotonic start time of the probe: a result
        that started before a forced switch must not count towards it.
        """
        key = (task.serial, task.name, task.side)
        with self._lock:
            self._in_flight.discard(key)
            states = self._versions.get(task.serial)
            if states is None:
                return  # version changed while the probe was in flight
            state = states.get(task.name)
            if state is None:
                return
            side = state.primary if task.side == "primary" else state.backup
            # Guard against a probe for a superseded target of the same
            # serial (should not happen, defensive only).
            expected_ip = (
                state.primary_ip
                if task.side == "primary"
                else state.backup_ip
            )
            if task.ip != expected_ip:
                return
            if task.side == "primary" and not_before < state.epoch:
                return
            side.latest_ok = ok
            if ok:
                side.ok_streak += 1
                side.fail_streak = 0
            else:
                side.fail_streak += 1
                side.ok_streak = 0

    # -- answering -----------------------------------------------------
    def decide(self, serial: int, name: str) -> Decision:
        """Pick the address to answer for ``name`` at version ``serial``.

        All state used here belongs to ``serial`` itself; the registry is
        never consulted for a different version. ``Decision(None, None)``
        means SERVFAIL.
        """
        with self._lock:
            states = self._versions.get(serial)
            if states is None:
                return Decision(None, None)
            state = states.get(name)
            if state is None:
                return Decision(None, None)
            return self._decide_locked(state)

    @staticmethod
    def _decide_locked(state: ManagedState) -> Decision:
        p, b = state.primary, state.backup

        # Both addresses known to be unreachable -> no usable answer.
        if p.latest_ok is False and b.latest_ok is False:
            return Decision(None, None)

        if state.selected == "primary":
            if p.latest_ok is True:
                return Decision(state.primary_ip, "primary")
            if p.latest_ok is False:
                if p.fail_streak >= 2 and b.latest_ok is True:
                    state.selected = "backup"
                    log.info(
                        "failover %s: primary -> backup (primary %s failed "
                        "%s times)",
                        state.name, state.primary_ip, p.fail_streak,
                    )
                    return Decision(state.backup_ip, "backup")
                if p.fail_streak < 2 and b.latest_ok is True:
                    # Not yet two consecutive failures: keep primary.
                    return Decision(state.primary_ip, "primary")
            # Unknown primary result (or failed twice without reachable
            # backup): no valid probe result -> SERVFAIL.
            return Decision(None, None)

        # selected == "backup"
        if p.latest_ok is True and p.ok_streak >= 2:
            state.selected = "primary"
            log.info(
                "failover %s: backup -> primary (primary %s succeeded %s "
                "times)",
                state.name, state.primary_ip, p.ok_streak,
            )
            return Decision(state.primary_ip, "primary")
        if b.latest_ok is True:
            return Decision(state.backup_ip, "backup")
        # Backup is down/unknown and primary is not yet proven twice:
        # answer neither address.
        return Decision(None, None)

    # -- introspection -------------------------------------------------
    def force_backup(self, serial: int, name: str) -> bool:
        """Explicit operator override to the backup address.

        Only touches the state of the given (active) version. Returns
        False when the version/record is unknown or not managed.
        """
        with self._lock:
            states = self._versions.get(serial)
            if states is None:
                return False
            state = states.get(name)
            if state is None:
                return False
            state.selected = "backup"
            # Count recovery from scratch: only two consecutive primary
            # successes from probes started *after* the operator action
            # may fail back.
            state.epoch = time.monotonic()
            state.primary.latest_ok = None
            state.primary.ok_streak = 0
            if state.backup.latest_ok is None:
                # The backup was never probed: let the operator's explicit
                # override answer it unless a probe proves it down.
                state.backup.latest_ok = True
            return True

    def describe(self, serial: int) -> List[Dict[str, Any]]:
        with self._lock:
            states = self._versions.get(serial) or {}
            out: List[Dict[str, Any]] = []
            for state in sorted(states.values(), key=lambda s: s.name):
                decision = self._decide_locked(state)
                out.append(
                    {
                        "name": state.name,
                        "primary_ip": state.primary_ip,
                        "backup_ip": state.backup_ip,
                        "port": state.port,
                        "active": decision.active,
                        "answer": decision.answer,
                        "primary": {
                            "latest_ok": state.primary.latest_ok,
                            "ok_streak": state.primary.ok_streak,
                            "fail_streak": state.primary.fail_streak,
                        },
                        "backup": {
                            "latest_ok": state.backup.latest_ok,
                            "ok_streak": state.backup.ok_streak,
                            "fail_streak": state.backup.fail_streak,
                        },
                    }
                )
            return out

    def reset(self) -> None:
        with self._lock:
            self._serial = None
            self._versions = {}
            self._in_flight = set()


class Prober:
    """Background TCP probe loop, independent from HTTP sync and DNS."""

    def __init__(self, registry: FailoverRegistry) -> None:
        self.registry = registry
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="probe-loop", daemon=True
        )
        self._thread.start()
        log.info(
            "probe loop started (interval=%ss timeout=%ss workers=%s)",
            settings.probe_interval, settings.probe_timeout,
            settings.probe_workers,
        )

    def kick(self) -> None:
        """Request an immediate probe round (e.g. right after a publish).

        Only wakes the loop; the caller never waits for a probe to finish.
        """
        self._wake.set()

    def _run(self) -> None:
        from .storage import storage

        executor = ThreadPoolExecutor(
            max_workers=max(1, settings.probe_workers),
            thread_name_prefix="probe",
        )
        try:
            while not self._stop.is_set():
                started = time.monotonic()
                try:
                    serial, records, _ = storage.get_active_version()
                    tasks = self.registry.observe(serial, records)
                    for task in tasks:
                        if self.registry.begin_probe(task):
                            launched = time.monotonic()
                            executor.submit(
                                self._run_one, task, launched
                            )
                except Exception as exc:  # pragma: no cover - defensive
                    log.exception("probe tick failed: %s", exc)
                elapsed = time.monotonic() - started
                # Wait for the interval or an explicit kick (publish/sync),
                # so a new version is probed immediately without the HTTP
                # path ever waiting for a probe.
                self._wake.wait(
                    max(0.0, settings.probe_interval - elapsed)
                )
                self._wake.clear()
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    def _run_one(self, task: ProbeTask, started: float) -> None:
        ok = tcp_probe(task.ip, task.port, settings.probe_timeout)
        self.registry.record_result(task, ok, not_before=started)

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None


registry = FailoverRegistry()
prober = Prober(registry)
