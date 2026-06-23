"""Unit tests for the aggregate transform (TT.1).

Run in an environment with pyspark==3.5.6 and Java 17:
    pip install -r tests/requirements-dev.txt
    pytest tests/
"""
import aggregate


def test_transform_dedups_and_aggregates(sample_events):
    out = {
        r["category"]: r
        for r in aggregate.transform(sample_events, "2026-06-01").collect()
    }
    assert out["books"]["cnt"] == 2
    assert abs(out["books"]["total"] - 15.5) < 1e-9
    assert out["music"]["cnt"] == 2
    assert abs(out["music"]["total"] - 10.0) < 1e-9
    assert all(r["run_date"] == "2026-06-01" for r in out.values())


def test_transform_is_idempotent(sample_events):
    first = sorted(tuple(r) for r in aggregate.transform(sample_events, "2026-06-01").collect())
    second = sorted(tuple(r) for r in aggregate.transform(sample_events, "2026-06-01").collect())
    assert first == second


def test_transform_salted_matches_unsalted(sample_events):
    # Salting (Design §9) must not change results, only the shuffle distribution.
    base = {r["category"]: r for r in aggregate.transform(sample_events, "2026-06-01").collect()}
    salted = {
        r["category"]: r
        for r in aggregate.transform(sample_events, "2026-06-01", salt=8).collect()
    }
    assert base.keys() == salted.keys()
    for cat in base:
        assert base[cat]["cnt"] == salted[cat]["cnt"]
        assert abs(base[cat]["total"] - salted[cat]["total"]) < 1e-9
