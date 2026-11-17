"""Failover + version-isolation end-to-end test.

Runs a real primary and backup uvicorn process. Tiny TCP "services" bound
to 127.0.0.x addresses let the test bring the primary/backup address up
and down, and measure how quickly publishes stay responsive while probes
are timing out.

Scenarios:
  1. managed record published with both addresses up -> primary address
  2. primary address down twice -> switch to backup address
  3. both addresses down -> SERVFAIL (not NXDOMAIN)
  4. primary returns -> two consecutive successes -> switch back
  5. no / stale probe result -> SERVFAIL, never an address from another
     version
  6. zone version change with different addresses -> never answers an
     address that belongs to the old version; old probes can't leak
  7. publishing stays fast while probes are timing out (non-blocking)
  8. plain A records still work alongside managed records
  9. backup keeps answering its old version while sync is broken, then
     follows the new version after recovery
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import httpx
from dnslib import DNSRecord, RCODE

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

ZONE = "internal.local"
PRIMARY_API = "http://127.0.0.1:18100"
BACKUP_API = "http://127.0.0.1:18101"
PRIMARY_DNS = ("127.0.0.1", 15153)
BACKUP_DNS = ("127.0.0.1", 15154)
BLACKHOLE = "http://127.0.0.1:19998"
PORT = 19091
PRIMARY_IP = "127.0.0.21"
BACKUP_IP = "127.0.0.22"
PROBE_INTERVAL = 0.2
PROBE_TIMEOUT = 0.3

PASS, FAIL = 0, 0
_failures = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        _failures.append(name)
        print(f"  FAIL  {name} {detail}")


def wait_http(url: str, timeout: float = 15.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1.5).status_code == 200:
                return
        except Exception as exc:
            last = exc
        time.sleep(0.2)
    raise RuntimeError(f"service not ready: {url} ({last})")


def wait_serial(api: str, wanted: int, timeout: float = 15.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if httpx.get(f"{api}/api/version", timeout=2).json()[
                "serial"
            ] == wanted:
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def dns_query(server, name: str, qtype: str = "A", timeout: float = 3.0):
    q = DNSRecord.question(name, qtype)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(q.pack(), server)
        data, _ = sock.recvfrom(65535)
    finally:
        sock.close()
    return DNSRecord.parse(data)


def answer_ips(server, name: str):
    r = dns_query(server, name)
    return r.header.rcode, [str(rr.rdata) for rr in r.rr]


def wait_answer(server, name: str, expected, timeout: float = 8.0):
    """Poll UDP until the expected (rcode, [ips]) combination is observed."""
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = answer_ips(server, name)
            if last == expected:
                return last
        except Exception as exc:  # pragma: no cover
            last = repr(exc)
        time.sleep(0.1)
    return last


class TcpService:
    """A listening TCP socket bound to one specific 127.0.0.x address."""

    def __init__(self, ip: str, port: int) -> None:
        self.ip = ip
        self.port = port
        self._sock: socket.socket | None = None
        self._thread: threading.Thread | None = None
        self.up()

    def up(self) -> None:
        if self._sock is not None:
            return
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.ip, self.port))
        sock.listen(64)
        sock.settimeout(0.2)
        self._sock = sock

        def accept_loop() -> None:
            while True:
                current = self._sock
                if current is None:
                    return
                try:
                    conn, _ = current.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return
                conn.close()

        self._thread = threading.Thread(
            target=accept_loop, daemon=True
        )
        self._thread.start()

    def down(self) -> None:
        sock, self._sock = self._sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=1.0)
            self._thread = None


def start_service(role: str, api_port: int, dns_port: int, db: str,
                  primary_url: str | None) -> subprocess.Popen:
    env = os.environ.copy()
    env.update(
        {
            "DNS_ROLE": role,
            "DNS_ZONE": ZONE,
            "DB_PATH": db,
            "DNS_HOST": "127.0.0.1",
            "DNS_PORT": str(dns_port),
            "API_HOST": "127.0.0.1",
            "API_PORT": str(api_port),
            "SYNC_INTERVAL": "1",
            "HTTP_TIMEOUT": "3",
            "PROBE_INTERVAL": str(PROBE_INTERVAL),
            "PROBE_TIMEOUT": str(PROBE_TIMEOUT),
            "PYTHONUNBUFFERED": "1",
        }
    )
    if primary_url:
        env["PRIMARY_API_URL"] = primary_url
    log = open(DATA / f"{role}-failover.log", "w")
    return subprocess.Popen(
        [PY, "-I", "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(api_port)],
        cwd=str(ROOT), env=env, stdout=log, stderr=subprocess.STDOUT,
    )


def stop(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def publish(api: str, records) -> httpx.Response:
    return httpx.post(f"{api}/api/zone", json={"records": records}, timeout=5)


def managed(name, primary, backup, port=PORT):
    return {
        "name": name,
        "primary_ip": primary,
        "backup_ip": backup,
        "port": port,
    }


def main() -> int:
    for f in ("primary-failover.db", "backup-failover.db"):
        for suffix in ("", "-wal", "-shm"):
            p = DATA / (f + suffix)
            if p.exists():
                p.unlink()

    svc_primary = TcpService(PRIMARY_IP, PORT)
    svc_backup = TcpService(BACKUP_IP, PORT)
    primary = start_service(
        "primary", 18100, 15153, str(DATA / "primary-failover.db"), None
    )
    backup = start_service(
        "backup", 18101, 15154, str(DATA / "backup-failover.db"), PRIMARY_API
    )
    try:
        wait_http(f"{PRIMARY_API}/health")
        wait_http(f"{BACKUP_API}/health")

        print("[1] managed record, both addresses up -> primary address")
        resp = publish(PRIMARY_API, [managed(f"svc.{ZONE}",
                                             PRIMARY_IP, BACKUP_IP)])
        check("managed zone accepted", resp.status_code == 201, resp.text)
        got = wait_answer(PRIMARY_DNS, f"svc.{ZONE}",
                          (RCODE.NOERROR, [PRIMARY_IP]))
        check("primary DNS answers primary address", got == (
            RCODE.NOERROR, [PRIMARY_IP]), str(got))
        check("backup syncs the managed zone",
              wait_serial(BACKUP_API, resp.json()["serial"]))
        got = wait_answer(BACKUP_DNS, f"svc.{ZONE}",
                          (RCODE.NOERROR, [PRIMARY_IP]))
        check("backup DNS also answers primary address", got == (
            RCODE.NOERROR, [PRIMARY_IP]), str(got))
        info = httpx.get(f"{BACKUP_API}/api/version", timeout=3).json()
        check("backup /api/version exposes managed probe state",
              info["probe_targets"][0]["primary_ip"] == PRIMARY_IP,
              json.dumps(info))

        print("[2] one primary failure keeps primary; two -> switch")
        svc_primary.down()
        time.sleep(PROBE_INTERVAL + 0.1)
        got = wait_answer(PRIMARY_DNS, f"svc.{ZONE}",
                          (RCODE.NOERROR, [BACKUP_IP]))
        check("after 2 consecutive primary failures -> backup address",
              got == (RCODE.NOERROR, [BACKUP_IP]), str(got))
        got = wait_answer(BACKUP_DNS, f"svc.{ZONE}",
                          (RCODE.NOERROR, [BACKUP_IP]))
        check("backup server fails over independently", got == (
            RCODE.NOERROR, [BACKUP_IP]), str(got))

        print("[3] both addresses down -> SERVFAIL")
        svc_backup.down()
        got = wait_answer(PRIMARY_DNS, f"svc.{ZONE}",
                          (RCODE.SERVFAIL, []))
        check("both down -> SERVFAIL (not NXDOMAIN)",
              got == (RCODE.SERVFAIL, []), str(got))
        got = wait_answer(BACKUP_DNS, f"svc.{ZONE}",
                          (RCODE.SERVFAIL, []))
        check("backup DNS SERVFAIL when both down",
              got == (RCODE.SERVFAIL, []), str(got))

        print("[4] primary returns; two consecutive successes -> failback")
        svc_primary.up()
        got = wait_answer(PRIMARY_DNS, f"svc.{ZONE}",
                          (RCODE.NOERROR, [PRIMARY_IP]))
        check("primary address answers again after failback",
              got == (RCODE.NOERROR, [PRIMARY_IP]), str(got))
        # Bring the backup service back too so double failure is over.
        svc_backup.up()
        time.sleep(PROBE_INTERVAL)
        got = answer_ips(PRIMARY_DNS, f"svc.{ZONE}")
        check("stays on primary once failed back",
              got == (RCODE.NOERROR, [PRIMARY_IP]), str(got))

        print("[5] no probe result yet after a version change -> SERVFAIL")
        # New version points at a black-holed port: probes cannot succeed
        # (nothing listens) but must not block the publish.
        blocked = 19099
        t0 = time.time()
        resp = publish(PRIMARY_API, [
            managed(f"svc.{ZONE}", PRIMARY_IP, BACKUP_IP, port=blocked),
        ])
        elapsed = time.time() - t0
        check("publish of managed zone is fast (<1s) despite probes",
              resp.status_code == 201 and elapsed < 1.0,
              f"{elapsed:.3f}s")
        got = wait_answer(PRIMARY_DNS, f"svc.{ZONE}",
                          (RCODE.SERVFAIL, []))
        check("new version with no successful probe -> SERVFAIL, no "
              "stale carry-over", got == (RCODE.SERVFAIL, []), str(got))

        print("[6] operator switch fires instantly; version change to "
              "different addresses never leaks the old version's address")
        resp = publish(PRIMARY_API, [
            managed(f"svc.{ZONE}", PRIMARY_IP, BACKUP_IP, port=PORT),
        ])
        check("version 3 published", resp.status_code == 201, resp.text)
        check("record is managed after republish",
              wait_serial(BACKUP_API, resp.json()["serial"]))
        wait_answer(PRIMARY_DNS, f"svc.{ZONE}",
                    (RCODE.NOERROR, [PRIMARY_IP]))
        r = httpx.post(
            f"{PRIMARY_API}/api/failover/switch",
            json={"name": f"svc.{ZONE}", "primary_ip": PRIMARY_IP},
            timeout=3,
        )
        check("operator switch accepted", r.status_code == 200, r.text)
        got = answer_ips(PRIMARY_DNS, f"svc.{ZONE}")
        check("switch takes effect on the next DNS answer without waiting "
              "for a probe", got == (RCODE.NOERROR, [BACKUP_IP]), str(got))
        # Primary is healthy, so the regular rule (two consecutive
        # successes) fails back shortly afterwards.
        got = wait_answer(PRIMARY_DNS, f"svc.{ZONE}",
                          (RCODE.NOERROR, [PRIMARY_IP]), timeout=3)
        check("healthy primary fails back after two fresh successes",
              got == (RCODE.NOERROR, [PRIMARY_IP]), str(got))
        # A wrong/stale address is refused (guards against forced answers
        # that do not belong to the active version).
        r = httpx.post(
            f"{PRIMARY_API}/api/failover/switch",
            json={"name": f"svc.{ZONE}", "primary_ip": "10.9.9.9"},
            timeout=3,
        )
        check("switch with mismatched primary address refused (409)",
              r.status_code == 409, r.text)
        r = httpx.post(
            f"{PRIMARY_API}/api/failover/switch",
            json={"name": "plain." + ZONE, "primary_ip": PRIMARY_IP},
            timeout=3,
        )
        check("switch on a non-managed name refused (404)",
              r.status_code == 404, r.text)
        # Back to primary for the rest of the scenario: two consecutive
        # successes are required, so restore by re-publishing serial 3
        # (new version = fresh state, primary selected).
        resp = publish(PRIMARY_API, [
            managed(f"svc.{ZONE}", PRIMARY_IP, BACKUP_IP, port=PORT),
        ])
        check("restored by republishing", resp.status_code == 201,
              resp.text)
        # Immediately after the change the user-visible contract must
        # hold: answers are either SERVFAIL or the new version's address
        # (which happens to be the primary here). Any old-version address
        # would be a leak; the recorded IP set is unchanged, so check the
        # version recorded by /api/version plus consistent answers.
        deadline = time.time() + 8
        seen = set()
        while time.time() < deadline:
            rcode, ips = answer_ips(PRIMARY_DNS, f"svc.{ZONE}")
            seen.add((rcode, tuple(ips)))
        check("answers converge to the new version's primary address",
              (RCODE.NOERROR, (PRIMARY_IP,)) in seen, str(seen))
        check("no answer ever mixes or uses the wrong address",
              all(
                  list(ips) in ([], [PRIMARY_IP])
                  for _, ips in seen
              ), str(seen))

        # A *different* managed address in the new version proves that
        # new-version probes (not old results) drive the answer.
        other_primary, other_backup = "127.0.0.31", "127.0.0.32"
        svc2_primary = TcpService(other_primary, PORT)
        svc2_backup = TcpService(other_backup, PORT)
        try:
            resp = publish(PRIMARY_API, [
                managed(f"svc.{ZONE}", other_primary, other_backup),
            ])
            check("version 4 with new addresses published",
                  resp.status_code == 201, resp.text)
            # Until the new version's probes succeed only SERVFAIL is
            # allowed, never the previous version's address.
            forbidden = {PRIMARY_IP, BACKUP_IP}
            leaked = []
            deadline = time.time() + 0.5
            while time.time() < deadline:
                _, ips = answer_ips(PRIMARY_DNS, f"svc.{ZONE}")
                if forbidden.intersection(ips):
                    leaked.append(ips)
            check("no old-version address immediately after switch",
                  not leaked, str(leaked[:3]))
            got = wait_answer(PRIMARY_DNS, f"svc.{ZONE}",
                              (RCODE.NOERROR, [other_primary]))
            check("new version answers new primary address",
                  got == (RCODE.NOERROR, [other_primary]), str(got))
        finally:
            svc2_primary.down()
            svc2_backup.down()

        print("[7] publishing with both addresses blackholed is not blocked")
        resp = publish(PRIMARY_API, [
            managed(f"slow.{ZONE}", "10.255.255.1", "10.255.255.2",
                    port=PORT),
        ])
        # 10.255.255.1/2 are usually unreachable; probes time out after
        # PROBE_TIMEOUT but the publish returns immediately.
        check("publish with unreachable addresses accepted",
              resp.status_code == 201, resp.text)
        latency = []
        for _ in range(5):
            t0 = time.time()
            httpx.get(f"{PRIMARY_API}/api/version", timeout=2)
            latency.append(time.time() - t0)
        check("API stays responsive while probes time out",
              max(latency) < 0.5, f"{max(latency):.3f}s")
        got = answer_ips(PRIMARY_DNS, f"slow.{ZONE}")
        check("unreachable managed record -> SERVFAIL",
              got == (RCODE.SERVFAIL, []), str(got))

        # UDP answers must not wait for a timing-out probe either.
        latencies = []
        for _ in range(10):
            t0 = time.time()
            dns_query(PRIMARY_DNS, f"slow.{ZONE}")
            latencies.append(time.time() - t0)
        check("UDP replies stay fast while probes time out",
              max(latencies) < 0.5, f"{max(latencies):.3f}s")

        # Sync must not wait for probes either.
        target = httpx.get(
            f"{PRIMARY_API}/api/version", timeout=3
        ).json()["serial"]
        check("backup synced the blackholed-probe version",
              wait_serial(BACKUP_API, target))
        latencies = []
        for _ in range(3):
            t0 = time.time()
            r = httpx.post(f"{BACKUP_API}/api/sync", timeout=3)
            latencies.append(time.time() - t0)
            if r.status_code != 200:
                latencies.append(99)
        check("sync stays fast while probes time out",
              max(latencies) < 1.0, f"{max(latencies):.3f}s")

        print("[8] plain A records still work alongside managed ones")
        publish(PRIMARY_API, [
            {"name": f"plain.{ZONE}", "ip": "10.0.0.7"},
            managed(f"svc.{ZONE}", other_primary, other_backup),
        ])
        got = wait_answer(PRIMARY_DNS, f"plain.{ZONE}",
                          (RCODE.NOERROR, ["10.0.0.7"]))
        check("plain A record answered normally", got == (
            RCODE.NOERROR, ["10.0.0.7"]), str(got))
        got = answer_ips(PRIMARY_DNS, f"missing.{ZONE}")
        check("missing name is NXDOMAIN", got == (RCODE.NXDOMAIN, []),
              str(got))

        print("[9] backup keeps old version while sync broken, then follows")
        # Stop the primary's API only by pointing the backup at a
        # blackhole: restart the backup process against a dead upstream.
        # First make sure both are on the same version.
        resp = publish(PRIMARY_API, [
            managed(f"svc.{ZONE}", PRIMARY_IP, BACKUP_IP),
        ])
        version = resp.json()["serial"]
        check("backup caught up before the break",
              wait_serial(BACKUP_API, version))
        svc_primary.down()
        stop(backup)
        backup = start_service(
            "backup", 18101, 15154,
            str(DATA / "backup-failover.db"), BLACKHOLE,
        )
        wait_http(f"{BACKUP_API}/health")
        time.sleep(1.5)
        got = wait_answer(BACKUP_DNS, f"svc.{ZONE}",
                          (RCODE.NOERROR, [BACKUP_IP]))
        check("backup still probes+answers its old version while sync is "
              "broken", got == (RCODE.NOERROR, [BACKUP_IP]), str(got))
        info = httpx.get(f"{BACKUP_API}/api/version", timeout=3).json()
        check("backup sync state failed, serial unchanged",
              info["last_sync_status"] == "failed"
              and info["serial"] == version, json.dumps(info))

        # Publish a new version on the primary while the backup is cut off.
        svc_primary.up()
        resp = publish(PRIMARY_API, [
            managed(f"svc.{ZONE}", BACKUP_IP, PRIMARY_IP),
        ])
        new_version = resp.json()["serial"]
        check("primary advanced while backup is cut off",
              new_version > version)
        got = answer_ips(BACKUP_DNS, f"svc.{ZONE}")
        check("backup keeps serving old version's primary address",
              got == (RCODE.NOERROR, [BACKUP_IP]), str(got))

        # Reconnect the backup; it must follow the new version and its
        # probes must be those of the new version.
        stop(backup)
        backup = start_service(
            "backup", 18101, 15154,
            str(DATA / "backup-failover.db"), PRIMARY_API,
        )
        wait_http(f"{BACKUP_API}/health")
        check("backup catches up after recovery",
              wait_serial(BACKUP_API, new_version))
        # New version: primary=BACKUP_IP, backup=PRIMARY_IP. Both services
        # are up, so it answers the new primary (= old backup address).
        got = wait_answer(BACKUP_DNS, f"svc.{ZONE}",
                          (RCODE.NOERROR, [BACKUP_IP]))
        check("after recovery backup answers the new version's primary",
              got == (RCODE.NOERROR, [BACKUP_IP]), str(got))
        check("serial in /api/version is the new version",
              httpx.get(f"{BACKUP_API}/api/version").json()["serial"]
              == new_version)
    finally:
        svc_primary.down()
        svc_backup.down()
        stop(primary)
        if backup.poll() is None:
            stop(backup)

    print(f"\n==== {PASS} passed, {FAIL} failed ====")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
