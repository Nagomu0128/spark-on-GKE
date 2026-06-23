# Skew experiment (Phase 5 / Design §9)

Reproduce data skew and measure the effect of **salting**, the second core theme
of this project (the first is idempotency, §8).

## Idea

`aggregate.py` does `groupBy("category")`. If one category holds most of the
rows, the shuffle that feeds the aggregation sends almost everything to a single
reducer task — one task runs far longer than the rest and dictates the whole
job's wall-clock. **Salting** splits the hot key into `N` random sub-groups
(`category, salt`), aggregates those in parallel, then sums the partials back per
category. The result is identical (verified by a unit test); the shuffle is
balanced.

AQE (`spark.sql.adaptive.*`, already on in the manifests) helps coalesce small
partitions and rebalance some skew, but a heavily skewed `groupBy` key is the
case salting targets directly.

## Run it

The image bakes `jobs/`, so rebuild & push it first so `gen_skewed.py` and
`aggregate.py --salt` are present in the image the SparkApplications pull:

```sh
cd infra
./image.sh                             # rebuild & push spark-gcs (bakes jobs/)
RUN_DATE=2026-06-02 ./run-skew.sh      # use a date distinct from real data
```

`run-skew.sh`:

1. `gen_skewed.py` writes a skewed slice to `raw/events/dt=2026-06-02/`
   (default 2,000,000 rows, 90% in category `hot`).
2. runs the baseline aggregate (`--salt 1`, name `agg-salt1-20260602`),
3. runs the salted aggregate (`--salt 16`, name `agg-salt16-20260602`).

Both write the same `staging/agg_by_category/run_date=2026-06-02/` (salting is
result-preserving); the comparison is in the History Server, not the output.

Tune the generator via `jobs/gen_skewed.py` args (`--rows`, `--hot-fraction`,
`--num-cold`) — render `manifests/skew-aggregate.yaml` / extend the script if you
want non-default sizes.

## Measure (DoD)

Open the History Server (see `manifests/history-server.yaml`, Phase 6) and
compare the two apps:

- **Stages tab → the shuffle/aggregate stage → Tasks**: for `agg-salt1`, one task
  has `Shuffle Read` and `Duration` far above the median (max/median ≫ 1) — the
  skew. For `agg-salt16`, task input and duration are roughly even.
- **Job wall-clock**: `agg-salt16` should finish faster than `agg-salt1` because
  no single task is the long pole.

Record the before/after wall-clock and the task max/median ratio; that
difference is the Phase 5 deliverable.

## Notes

- The salted path is exercised by `tests/test_aggregate.py`
  (`test_transform_salted_matches_unsalted`): salting must not change results.
- Clean up afterward: the synthetic raw slice and staging partition for the
  experiment `RUN_DATE` can be deleted from GCS once measured.
