# Observability (Phase 6 / Design §11)

Three layers: the **Spark History Server** (per-job stages/tasks), **Cloud
Logging** (driver/executor stdout/stderr), and optional **Prometheus/Grafana**
(operator metrics).

## Spark History Server

Event logs are written to `gs://<lake>/spark-events/` by every job
(`spark.eventLog.enabled=true` in the manifests). Run the server on demand:

```sh
cd infra
./history-server.sh                                            # apply + wait
kubectl -n spark-jobs port-forward svc/spark-history-server 18080:18080
# browse http://localhost:18080
./history-server.sh delete                                     # stop when done
```

It runs in `spark-jobs` under the `spark` KSA, so it reads GCS via Workload
Identity (no keys). Use it to inspect the **Stages → Tasks** view — this is how
you see data skew (one task with far more shuffle-read / longer duration than the
median; see `docs/skew-experiment.md`).

Run it on demand and delete it afterward — it does not need to be permanent.

## Cloud Logging (P6.2)

GKE ships container stdout/stderr to Cloud Logging automatically. View a job's
driver/executor logs without the cluster UI:

```sh
# Driver log for a specific SparkApplication driver pod
gcloud logging read \
  'resource.type="k8s_container" resource.labels.namespace_name="spark-jobs" resource.labels.pod_name="aggregate-20260601-driver"' \
  --limit 100 --freshness=1d --format='value(textPayload)'

# Or live, via kubectl while the pod exists:
kubectl -n spark-jobs logs <driver-or-executor-pod>
```

The DQ gate's pass/fail line (`validate_dq.py`) and `ingest_check` errors surface
here, so a failed Airflow task can be diagnosed from logs alone.

## Metrics (P6.3, optional)

The Kubeflow Spark Operator can expose Prometheus metrics (enable in its Helm
values) and Google Managed Prometheus (`gmp-system`, already on this cluster) can
scrape them; visualize in Cloud Monitoring / Grafana. Optional for this learning
project — the History Server + Cloud Logging cover the core needs.
