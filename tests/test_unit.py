"""Unit tests for validation and the SQLite version/rollback guarantees."""
import os
import tempfile

import pytest


@pytest.fixture()
def storage(tmp_path, monkeypatch):
    db = str(tmp_path / "test.db")
    monkeypatch.setenv("DB_PATH", db)
    monkeypatch.setenv("DNS_ZONE", "internal.local")
    monkeypatch.setenv("MAX_RECORDS", "8")
    # Import fresh so settings + storage bind to the temp env.
    import importlib

    from app import config, storage as storage_mod

    importlib.reload(config)
    importlib.reload(storage_mod)
    yield storage_mod
    storage_mod.storage._connect().close()


def rec(name, ip):
    return {"name": name, "ip": ip}


def test_seed_starts_at_serial_zero(storage):
    assert storage.storage.active_serial() == 0
    serial, records, _ = storage.storage.get_active_version()
    assert serial == 0 and records == []


def test_publish_increments_serial_only_on_success(storage):
    serial, _ = storage.storage.next_publish(
        [rec("db.internal.local", "10.0.0.1")]
    )
    assert serial == 1
    serial, _ = storage.storage.next_publish(
        [rec("db.internal.local", "10.0.0.2")]
    )
    assert serial == 2
    # Active version is exactly one immutable snapshot.
    active, records, _ = storage.storage.get_active_version()
    assert active == 2 and records == [rec("db.internal.local", "10.0.0.2")]


def test_stale_transfer_cannot_roll_back(storage):
    storage.storage.next_publish([rec("a.internal.local", "10.0.0.1")])
    storage.storage.commit_version(
        2, [rec("b.internal.local", "10.0.0.2")], "t2"
    )
    assert storage.storage.active_serial() == 2
    # Late arrival of an old complete version.
    applied = storage.storage.commit_version(
        1, [rec("old.internal.local", "10.9.9.9")], "t1-late"
    )
    assert applied is False
    assert storage.storage.active_serial() == 2
    _, records, _ = storage.storage.get_active_version()
    assert records == [rec("b.internal.local", "10.0.0.2")]
    # Re-delivery of the same serial is idempotent and rejected too.
    assert (
        storage.storage.commit_version(
            2, [rec("b.internal.local", "10.0.0.2")], "t2-dup"
        )
        is False
    )


def test_failed_commit_leaves_old_version(storage):
    storage.storage.next_publish([rec("a.internal.local", "10.0.0.1")])
    with pytest.raises(Exception):
        # serial < 1 is invalid; nothing should have changed.
        storage.storage.commit_version(
            0, [rec("x.internal.local", "10.0.0.9")], "bad"
        )
    assert storage.storage.active_serial() == 1


def test_zone_validation_rules():
    from app.models import ZonePayload, normalise_zone_records

    def payload(rows):
        return ZonePayload.model_validate(
            {"records": [{"name": n, "ip": ip} for n, ip in rows]}
        )

    ok = payload(
        [
            ("db.internal.local", "10.0.0.1"),
            ("web.internal.local", "10.0.0.2"),
        ]
    )
    assert len(ok.records) == 2

    with pytest.raises(Exception):
        payload([])  # empty zone
    with pytest.raises(Exception):
        payload([("db.external.example", "10.0.0.1")])  # outside zone
    with pytest.raises(Exception):
        payload([("db.internal.local", "not-an-ip")])
    with pytest.raises(Exception):
        payload([("db.internal.local", "fe80::1")])  # IPv6 not allowed
    with pytest.raises(Exception):
        payload(
            [
                ("db.internal.local", "10.0.0.1"),
                ("db.internal.local", "10.0.0.2"),
            ]
        )  # duplicate name
    with pytest.raises(Exception):
        payload(
            [(f"h{i}.internal.local", f"10.0.0.{i}") for i in range(9)]
        )  # > 8 records

    # Bare apex and unqualified relative names inside the zone are fine.
    apex = payload([("internal.local", "10.0.0.1")])
    assert apex.records[0].name == "internal.local"
    trailing = payload([("svc.internal.local.", "10.0.0.5")])
    assert trailing.records[0].name == "svc.internal.local"

    # -- managed A records ---------------------------------------------
    managed = ZonePayload.model_validate(
        {
            "records": [
                {"name": "plain.internal.local", "ip": "10.0.0.9"},
                {
                    "name": "mixed.internal.local",
                    "primary_ip": "10.0.1.1",
                    "backup_ip": "10.0.1.2",
                    "port": 5432,
                },
            ]
        }
    )
    assert managed.to_storage_records() == [
        {
            "name": "mixed.internal.local",
            "primary_ip": "10.0.1.1",
            "backup_ip": "10.0.1.2",
            "port": 5432,
        },
        {"name": "plain.internal.local", "ip": "10.0.0.9"},
    ]

    def bad_zone(*records):
        with pytest.raises(Exception):
            normalise_zone_records(list(records))

    bad_zone(
        {
            "name": "h.internal.local",
            "primary_ip": "10.0.1.1",
            "backup_ip": "10.0.1.2",
        }
    )  # managed record without port
    bad_zone(
        {
            "name": "h.internal.local",
            "primary_ip": "10.0.1.1",
            "backup_ip": "10.0.1.1",
            "port": 5432,
        }
    )  # identical addresses
    bad_zone(
        {
            "name": "h.internal.local",
            "ip": "10.0.0.1",
            "primary_ip": "10.0.1.1",
            "backup_ip": "10.0.1.2",
            "port": 5432,
        }
    )  # mixed plain + managed in one record
    bad_zone(
        {
            "name": "h.external.example",
            "primary_ip": "10.0.1.1",
            "backup_ip": "10.0.1.2",
            "port": 5432,
        }
    )  # outside zone
    bad_zone(
        {
            "name": "h.internal.local",
            "primary_ip": "10.0.1.1",
            "backup_ip": "fe80::2",
            "port": 5432,
        }
    )  # IPv6 backup address
    bad_zone(
        {
            "name": "h.internal.local",
            "primary_ip": "10.0.1.1",
            "backup_ip": "10.0.1.2",
            "port": 0,
        }
    )  # port out of range
    bad_zone(
        {
            "name": "h.internal.local",
            "primary_ip": "10.0.1.1",
            "backup_ip": "10.0.1.2",
            "port": 70000,
        }
    )
    # A plain record may not collide with a managed record name.
    bad_zone(
        {"name": "h.internal.local", "ip": "10.0.0.1"},
        {
            "name": "h.internal.local",
            "primary_ip": "10.0.1.1",
            "backup_ip": "10.0.1.2",
            "port": 5432,
        },
    )
    # The combined plain + managed count is limited to 8.
    with pytest.raises(Exception):
        normalise_zone_records(
            [{"name": f"h{i}.internal.local", "ip": f"10.0.0.{i}"}
             for i in range(8)]
            + [
                {
                    "name": "m.internal.local",
                    "primary_ip": "10.0.1.1",
                    "backup_ip": "10.0.1.2",
                    "port": 5432,
                }
            ]
        )

    # Exactly 8 mixed records is allowed.
    eight = normalise_zone_records(
        [{"name": f"h{i}.internal.local", "ip": f"10.0.0.{i}"}
         for i in range(7)]
        + [
            {
                "name": "m.internal.local",
                "primary_ip": "10.0.1.1",
                "backup_ip": "10.0.1.2",
                "port": 5432,
            }
        ]
    )
    assert len(eight) == 8


# -- failover state machine ------------------------------------------------
def _managed(serial=1, records=None):
    from app.failover import registry

    registry.reset()
    records = records or [
        {
            "name": "svc.internal.local",
            "primary_ip": "10.0.0.11",
            "backup_ip": "10.0.0.12",
            "port": 5432,
        }
    ]
    registry.observe(serial, records)
    return registry


def _result(registry, serial, side, ok):
    from app.failover import ProbeTask

    ip = "10.0.0.11" if side == "primary" else "10.0.0.12"
    registry.record_result(
        ProbeTask(serial, "svc.internal.local", side, ip, 5432), ok
    )


def test_no_probe_result_is_servfail():
    registry = _managed()
    decision = registry.decide(1, "svc.internal.local")
    assert decision.answer is None and decision.active is None


def test_single_primary_failure_keeps_primary():
    registry = _managed()
    _result(registry, 1, "primary", True)
    _result(registry, 1, "backup", True)
    assert registry.decide(1, "svc.internal.local").answer == "10.0.0.11"
    _result(registry, 1, "primary", False)
    d = registry.decide(1, "svc.internal.local")
    # Only one consecutive failure: keep answering the primary address.
    assert d.answer == "10.0.0.11" and d.active == "primary"


def test_two_primary_failures_with_reachable_backup_switch():
    registry = _managed()
    _result(registry, 1, "primary", True)
    _result(registry, 1, "backup", True)
    _result(registry, 1, "primary", False)
    _result(registry, 1, "primary", False)
    d = registry.decide(1, "svc.internal.local")
    assert d.answer == "10.0.0.12" and d.active == "backup"


def test_two_failures_without_reachable_backup_servfail():
    registry = _managed()
    _result(registry, 1, "primary", True)
    _result(registry, 1, "backup", False)
    _result(registry, 1, "primary", False)
    _result(registry, 1, "primary", False)
    assert registry.decide(1, "svc.internal.local").answer is None


def test_failback_needs_two_consecutive_successes():
    registry = _managed()
    _result(registry, 1, "primary", True)
    _result(registry, 1, "backup", True)
    _result(registry, 1, "primary", False)
    _result(registry, 1, "primary", False)
    assert registry.decide(1, "svc.internal.local").active == "backup"
    _result(registry, 1, "primary", True)
    # One success is not enough, even though the primary is reachable.
    d = registry.decide(1, "svc.internal.local")
    assert d.answer == "10.0.0.12" and d.active == "backup"
    _result(registry, 1, "primary", True)
    d = registry.decide(1, "svc.internal.local")
    assert d.answer == "10.0.0.11" and d.active == "primary"


def test_failback_successes_must_be_consecutive():
    registry = _managed()
    _result(registry, 1, "primary", True)
    _result(registry, 1, "backup", True)
    _result(registry, 1, "primary", False)
    _result(registry, 1, "primary", False)
    assert registry.decide(1, "svc.internal.local").active == "backup"
    _result(registry, 1, "primary", True)
    _result(registry, 1, "primary", False)  # streak reset
    _result(registry, 1, "primary", True)
    assert registry.decide(1, "svc.internal.local").active == "backup"
    _result(registry, 1, "primary", True)
    assert registry.decide(1, "svc.internal.local").active == "primary"


def test_both_down_servfail_while_selected_primary():
    registry = _managed()
    _result(registry, 1, "primary", False)
    _result(registry, 1, "backup", False)
    assert registry.decide(1, "svc.internal.local").answer is None


def test_new_version_discards_old_probe_results():
    registry = _managed(serial=1)
    _result(registry, 1, "primary", True)
    _result(registry, 1, "backup", True)
    _result(registry, 1, "primary", False)
    _result(registry, 1, "primary", False)
    assert registry.decide(1, "svc.internal.local").active == "backup"

    # Publish serial 2 with the *same* addresses: all counters restart and
    # no old result (including the failover selection) carries over.
    registry.observe(
        2,
        [
            {
                "name": "svc.internal.local",
                "primary_ip": "10.0.0.11",
                "backup_ip": "10.0.0.12",
                "port": 5432,
            }
        ],
    )
    d = registry.decide(2, "svc.internal.local")
    assert d.answer is None and d.active is None
    # A late result from a probe started under serial 1 is dropped.
    _result(registry, 1, "primary", True)
    _result(registry, 1, "backup", True)
    assert registry.decide(2, "svc.internal.local").answer is None


def test_probe_result_after_version_switch_cannot_leak():
    registry = _managed(serial=1)
    _result(registry, 1, "primary", True)
    _result(registry, 1, "backup", True)
    # serial 2 moves the primary to a different address.
    registry.observe(
        2,
        [
            {
                "name": "svc.internal.local",
                "primary_ip": "10.0.0.21",
                "backup_ip": "10.0.0.22",
                "port": 5432,
            }
        ],
    )
    from app.failover import ProbeTask

    # Old task finishing late: must not be applied to the new version.
    registry.record_result(
        ProbeTask(1, "svc.internal.local", "primary", "10.0.0.11", 5432),
        True,
    )
    registry.record_result(
        ProbeTask(2, "svc.internal.local", "primary", "10.0.0.11", 5432),
        True,
    )
    assert registry.decide(2, "svc.internal.local").answer is None
    registry.record_result(
        ProbeTask(2, "svc.internal.local", "primary", "10.0.0.21", 5432),
        True,
    )
    assert registry.decide(2, "svc.internal.local").answer == "10.0.0.21"
    # And the previous version still answers consistently from its own
    # (never mixed) state.
    assert registry.decide(1, "svc.internal.local").answer == "10.0.0.11"


def test_prober_fresh_version_is_not_in_flight_blocked():
    from app.failover import ProbeTask

    registry = _managed(serial=1)
    task = ProbeTask(1, "svc.internal.local", "primary", "10.0.0.11", 5432)
    assert registry.begin_probe(task) is True
    registry.observe(2, [
        {
            "name": "svc.internal.local",
            "primary_ip": "10.0.0.11",
            "backup_ip": "10.0.0.12",
            "port": 5432,
        }
    ])
    new_task = ProbeTask(2, "svc.internal.local", "primary", "10.0.0.11", 5432)
    assert registry.begin_probe(new_task) is True
