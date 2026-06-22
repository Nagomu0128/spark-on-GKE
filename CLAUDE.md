# spark-on-k8s

A **personal learning project** building a daily batch platform with **Spark on Kubernetes + GCS + self-hosted Airflow** on GKE.
`Design Doc.md` is the source of truth for the design. Read it first when in doubt (cited as §N below).

## Development harness (YOU MUST)

- **Never work directly on `main`.** Always cut a branch first: `feat/...` / `fix/...` / `docs/...` / `infra/...`.
- **Commit and push only when explicitly asked.** Do not commit or push on your own.
- **Confirm before destructive operations**: `git push --force`, `git reset --hard`, `gcloud ... delete` (clusters/buckets), `kubectl delete`.
- Commit messages: concise imperative (e.g. `add staging-publish task to DAG`). One concern per commit.
- **Verify before claiming done, and show evidence (the command and its output).** If you cannot verify it, do not say it works.
- When the design changes, **update `Design Doc.md` in the same PR** so code and design stay in sync.

## Secrets (IMPORTANT)

- **Never place or commit static service-account keys (JSON) in the repo.** GCS auth is **Workload Identity (ADC) only** (§10).
- Do not commit key files (`*-key.json`), `.env`, or `kubeconfig`. Keep secrets in Secret Manager / K8s Secret.

## Design invariants (code must not break these)

- **`run_date` is the logical date of the data being processed, not the run/execution date.** Read only that day's slice from `raw/<dataset>/dt=YYYY-MM-DD/` (§6.1, §8).
- **The main output path is staging→DQ→publish + a `_SUCCESS` sentinel.** Writing directly to curated (dynamic partition overwrite) is limited to local/small-data checks (§7, §8). GCS rename is non-atomic, so dynamic overwrite alone is not treated as the idempotency guarantee.
- **Idempotent**: re-running the same `run_date` any number of times yields a unique result. Write by overwrite/existence-check, not append (§8).
- **Pin versions**: Spark 3.5.x / Java 17 / `gcs-connector-hadoop3` pinned to a **specific version**. Do not use `-latest` (§15, §3.2 reproducibility).

## Cost (learning use, within $300 credit, no sustained operation)

- When done, **delete the whole cluster** (`gcloud container clusters delete`). Nothing runs permanently.
- Spark executors run on Spot with autoscaling **min 0**. The driver runs on a stable node pool (§6.2).

## Repository layout (as implementation progresses)

- `jobs/` — PySpark jobs (e.g. `aggregate.py`, §Appendix E)
- `dags/` — Airflow DAGs (§Appendix F)
- `manifests/` — K8s YAML such as SparkApplication (§Appendix D)
- `infra/` — gcloud / helm setup commands and scripts (§Appendix A, B)
- `Design Doc.md` — the design source of truth

## Verification (§12)

- Unit-test transform logic with **local Spark (`local[*]`) + pytest** on small fixed inputs.
- **Idempotency**: run the same `run_date` twice and confirm the output (row count / hash) matches.
- On GKE, verify with small data and inspect stages/task skew in the Spark History Server.

## Commands

Once the implementation stabilizes, add the real build / test / deploy commands here (not yet finalized).
Prefer CLI tools (`gcloud` / `kubectl` / `helm` / `gh`) for external services.
