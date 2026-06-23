# HDFS + YARN learning module (Phase 7 / Design Appendix G)

**Optional, learning only.** The main pipeline uses GCS, not HDFS (see the ADR,
Design §4). This module exists only to *feel* HDFS `put`/`get` and a MapReduce
job on YARN once. It is **not** part of the daily pipeline or CI, and it is
**not** meant to run permanently — deploy it, run one job, then delete it.

This is a minimal skeleton (1 NameNode, 1 DataNode, 1 ResourceManager, 1
NodeManager; replication 1). Image pinned to `apache/hadoop:3.3.6`
(also recorded in `infra/versions.env`).

## Capacity note

YARN's NodeManager can request up to ~5Gi (it holds the MapReduce containers).
The `system` node is small and may already run Airflow — free room first, e.g.
`helm uninstall airflow -n airflow`, before deploying this.

## Deploy

```sh
kubectl create namespace hdfs
kubectl apply -f manifests/hdfs/configmap.yaml
kubectl apply -f manifests/hdfs/hdfs.yaml      # NameNode + DataNode
kubectl apply -f manifests/hdfs/yarn.yaml      # ResourceManager + NodeManager
kubectl -n hdfs get pods -w                    # wait until all Running
```

## HDFS put / get (P7.1)

```sh
NN=hdfs-namenode-0
kubectl -n hdfs exec "$NN" -- /opt/hadoop/bin/hdfs dfs -mkdir -p /demo
echo "hello hadoop hello yarn hello hdfs" | \
  kubectl -n hdfs exec -i "$NN" -- /opt/hadoop/bin/hdfs dfs -put - /demo/words.txt
kubectl -n hdfs exec "$NN" -- /opt/hadoop/bin/hdfs dfs -cat /demo/words.txt
kubectl -n hdfs exec "$NN" -- /opt/hadoop/bin/hdfs dfs -ls /demo
```

## MapReduce wordcount on YARN (P7.1 DoD)

```sh
NN=hdfs-namenode-0
JAR=/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.6.jar
kubectl -n hdfs exec "$NN" -- /opt/hadoop/bin/yarn jar "$JAR" wordcount /demo /demo-out
kubectl -n hdfs exec "$NN" -- /opt/hadoop/bin/hdfs dfs -cat '/demo-out/part-r-00000'
# expect: hadoop 1 / hdfs 1 / hello 3 / yarn 1
```

Watch the job in the RM UI if you like:
`kubectl -n hdfs port-forward svc/hdfs-yarn-rm 8088:8088` → http://localhost:8088

## Teardown (do not leave running)

```sh
kubectl delete -f manifests/hdfs/yarn.yaml -f manifests/hdfs/hdfs.yaml -f manifests/hdfs/configmap.yaml
kubectl -n hdfs delete pvc --all       # release the pd-standard disks
kubectl delete namespace hdfs
```

## Caveats

This is a hand-written skeleton matching Appendix G; unlike the GCS pipeline
(Phases 1–6, verified on the cluster), it has **not** been run end-to-end here.
Hadoop-on-k8s image conventions (user/UID, paths, formatting) can vary by image
tag — if a daemon fails to start, check the pod logs and adjust the
`apache/hadoop` image's expected `HADOOP_HOME` / conf paths in the manifests.
