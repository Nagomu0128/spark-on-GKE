# 実装タスク分割 — spark-on-k8s

`docs/Design Doc.md` を実装に落とすための詳細タスク。各タスクに **成果物（ファイル/コマンド）** と **完了条件（DoD）** を付ける。フェーズは設計 §13 に対応し、より細かく分解している。

凡例：`[ ]` 未着手 / `[~]` 進行中 / `[x]` 完了。ID は `P<phase>.<n>`。依存は「← Px.y」で示す。

設計の不変条件（§6.1, §7, §8, §15）は全タスク共通の前提：
- `run_date` は**処理対象データの論理日付**（実行日ではない）。raw は `dt=YYYY-MM-DD/` の当日スライスのみ読む。
- 出力本線は **staging → DQ → publish ＋ `_SUCCESS`**。
- 冪等：同じ `run_date` の再実行で結果が一意。
- バージョンは固定（Spark 3.5.x / Java 17 / `gcs-connector-hadoop3` を具体版で）。`-latest` 禁止。
- 認証は Workload Identity（ADC）のみ。鍵をコミットしない。

---

## Phase 0 — 前提・リポジトリ整備

- [ ] **P0.1 GCP プロジェクト初期化**
  - 手順：プロジェクト選択/作成、課金紐付け、API 有効化（`container`, `artifactregistry`, `storage`, `iam`, `cloudresourcemanager`）、**予算アラート設定**。
  - 成果物：`infra/00-project.sh`
  - DoD：`gcloud config get-value project` が対象プロジェクト。予算アラート有効。
- [ ] **P0.2 ローカルツール準備**
  - 手順：`gcloud` / `kubectl` / `helm` / `docker` / `gh` のインストール・バージョン確認、`gcloud auth login` と ADC（`gcloud auth application-default login`）。
  - DoD：各 `--version` が表示され、ADC 取得済み。
- [ ] **P0.3 リポジトリ scaffolding**
  - 手順：`jobs/` `dags/` `manifests/` `infra/` `tests/` を作成。`.gitignore`（`*-key.json` / `.env` / `kubeconfig` / `__pycache__/` / `.venv/` / `*.pyc`）。`infra/versions.env` にピン版（Spark, GCS connector, Operator chart, Airflow chart, provider）を記録。
  - 成果物：ディレクトリ群、`.gitignore`、`infra/versions.env`
  - DoD：`.gitignore` とバージョン定義がコミットされている。

---

## Phase 1 — 基盤と疎通（GKE + GCS + Operator + SparkPi） ← P0

- [ ] **P1.1 変数定義** — `infra/01-vars.sh`（`PROJECT_ID` `REGION=asia-northeast1` `LAKE=gs://${PROJECT_ID}-datalake` `CLUSTER=spark-batch` 等）。
- [ ] **P1.2 GCS バケット作成** — `gcloud storage buckets create ${LAKE} --location ${REGION} --uniform-bucket-level-access`。プレフィックス規約（`raw/ staging/ curated/ spark-events/`）を README 化。DoD：バケット存在。
- [ ] **P1.3 GKE クラスタ作成** ← P1.1 — Standard・リージョナル・`--release-channel regular`・**Workload Identity 有効**（`--workload-pool=${PROJECT_ID}.svc.id.goog`）、system プール（on-demand, `e2-standard-4`, 1台）。成果物：`infra/02-cluster.sh`。DoD：`kubectl get nodes` 応答。
- [ ] **P1.4 Spark Spot ノードプール** ← P1.3 — `spark` プール（`--spot`, `--enable-autoscaling --min-nodes 0 --max-nodes N`, `n2-standard-8`, `--node-labels workload=spark`, `--node-taints workload=spark:NoSchedule`）。DoD：プール作成、アイドル時 0 台。
- [ ] **P1.5 namespace** — `spark-jobs` / `spark-operator` / `airflow`。
- [ ] **P1.6 Workload Identity 配線** ← P1.2,P1.5 — GSA `spark-gsa` 作成、バケットに `roles/storage.objectAdmin`、KSA `spark`（ns: spark-jobs）作成、`roles/iam.workloadIdentityUser` バインド、KSA に `iam.gke.io/gcp-service-account` アノテーション。成果物：`infra/03-workload-identity.sh`。DoD：テスト Pod が ADC で GCS list 成功（403 が出ない）。
- [ ] **P1.7 RBAC** ← P1.5 — `spark-role`/`spark-rb`（pods/services/configmaps/pvc の create 等）。成果物：`manifests/spark-rbac.yaml`。
- [ ] **P1.8 Spark Operator 導入** ← P1.5 — Helm（repo `kubeflow.github.io/spark-operator`, chart v2.x）、`spark.jobNamespaces={spark-jobs}`, `webhook.enable=true`。成果物：`infra/04-operator.sh`。DoD：operator Pod Running。
- [ ] **P1.9 SparkPi スモークテスト** ← P1.7,P1.8 — サンプル `SparkApplication`（spark-pi）を apply。成果物：`manifests/spark-pi.yaml`。**DoD（Phase1 完了）**：spark-pi が `Completed`、ジョブ後に spark プールが 0 台へ縮退。

---

## Phase 2 — 自前イメージ + 変換1本（raw → staging Parquet） ← P1

- [ ] **P2.1 Artifact Registry** — `gcloud artifacts repositories create spark --repository-format=docker --location=${REGION}`。
- [ ] **P2.2 自前イメージ** ← P2.1 — `FROM apache/spark:3.5.x`（ピン）、**ピン版 `gcs-connector-hadoop3-<x.y.z>-shaded.jar`** を `/opt/spark/jars/` へ、`COPY jobs/`。build & push。成果物：`Dockerfile`, `infra/05-image.sh`。DoD：イメージが AR に push 済み。
- [ ] **P2.3 データセット選定 & スキーマ定義** — Kaggle データ確定、**明示 `StructType` スキーマ**を定義（`inferSchema` 不使用）、ビジネスキーを特定。成果物：`jobs/schema.py`（or aggregate.py 内）。
- [ ] **P2.4 取り込み** ← P2.3 — CSV を `raw/<dataset>/dt=YYYY-MM-DD/` に配置する取り込みスクリプト（手動 or タスク）。成果物：`infra/ingest.sh` or `jobs/ingest.py`。DoD：raw に当日スライスが存在。
- [ ] **P2.5 `aggregate.py` 実装** ← P2.3 — 引数 `--run-date`、`raw/.../dt={run_date}/` を明示スキーマで読む、`dropDuplicates([business_key])`、`groupBy` 集計（count/sum）、`withColumn("run_date", lit(run_date))`、`coalesce(N)`、**`staging/<job>/` に `partitionBy("run_date")` で書く**（curated には書かない）。成果物：`jobs/aggregate.py`。
- [ ] **P2.6 SparkApplication マニフェスト** ← P2.2,P2.5 — ピン image、`sparkConf`（AQE 一式、`partitionOverwriteMode=dynamic`、`eventLog.dir=gs://.../spark-events/`、GCS コネクタ + ADC）、**driver は system プールへ明示配置（nodeSelector/affinity）**、executor は spot（nodeSelector + tolerations）、`restartPolicy`、`timeToLiveSeconds`。成果物：`manifests/spark_application.yaml`。
- [ ] **P2.7 手動実行 & 確認** ← P2.4,P2.6 — apply して staging に Parquet 出力、History Server でステージ確認。**DoD（Phase2 完了）**：staging に正しい Parquet が出る／SHS でステージが見える。

---

## Phase 3 — DQ + publish + 冪等性（本書の核） ← P2

- [ ] **P3.1 `validate_dq` 実装** — staging の出力を検証（行数 > 0、キー NULL 率 < 閾値、スキーマ一致）。閾値割れで失敗（publish させない）。成果物：`jobs/validate_dq.py`。
- [ ] **P3.2 `publish` 実装** ← P3.1 — `staging/<job>/<ds>/run_date=<ds>/` を `curated/<table>/run_date=<ds>/` に昇格し、**`_SUCCESS` センチネル**を設置。同一パーティション上書きで冪等。成果物：`jobs/publish.py`（or gsutil/手順）。
- [ ] **P3.3 冪等性テスト** ← P3.2 — 同じ `run_date` を 2 回流し、出力（件数/ハッシュ）が完全一致することを確認。成果物：`tests/test_idempotency.py`。
- [ ] **P3.4 読み手の `_SUCCESS` ゲート** — curated を読む側は `_SUCCESS` のあるパーティションのみ信頼する規約を明文化＋ヘルパ。
  - **DoD（Phase3 完了）**：同じ日付2回で結果一意／DQ 失敗時に curated が無傷。

---

## Phase 4 — オーケストレーション（Airflow） ← P3

- [ ] **P4.1 Airflow 導入** — Helm official chart、`executor=KubernetesExecutor`、DAG 配布（git-sync or イメージ同梱）。provider `apache-airflow-providers-cncf-kubernetes` を**バージョン固定**。成果物：`infra/06-airflow.sh`, `values-airflow.yaml`。DoD：Web UI 到達。
- [ ] **P4.2 K8s 接続** ← P4.1 — `kubernetes_default` 接続（in-cluster）設定。DoD：Airflow から spark-jobs に apply 可能。
- [ ] **P4.3 DAG `kaggle_batch`** ← P4.2 — タスク連鎖 `ingest_check → spark_aggregate(SparkKubernetesOperator) → validate_dq → publish`。`{{ ds }}` 伝播、`retries=2`、`catchup=True`、`max_active_runs=1`、**同名 SparkApplication の冪等 apply（apply 前 delete）**、完了待ち（sensor/deferrable）を provider 版に合わせて実装。成果物：`dags/kaggle_batch.py`, `dags/spark_application.yaml`。
- [ ] **P4.4 バックフィル検証** ← P4.3 — 過去日の範囲を流し、日付別パーティションが正しく作られることを確認。
  - **DoD（Phase4 完了）**：同じ日付2回で一意／バックフィルが動く。

---

## Phase 5 — スキュー再現と調整 ← P2（最低限）/ P4（理想）

- [ ] **P5.1 偏りデータ生成** — 特定キーに偏ったデータを raw に投入。成果物：`jobs/gen_skewed.py`。
- [ ] **P5.2 ベースライン計測** ← P5.1 — 対策前の wall-clock とタスク分布（max/median 比）を SHS で記録。
- [ ] **P5.3 緩和と比較** ← P5.2 — AQE `skewJoin`/`coalescePartitions`、`shuffle.partitions` 調整、salting（全集計指標を二段で再構成）を適用し改善を計測。
  - **DoD（Phase5 完了）**：対策前後の wall-clock 改善を SHS で確認。

---

## Phase 6 — 観測性とコスト ← P2

- [ ] **P6.1 History Server** — `gs://.../spark-events/` を参照（常設せず必要時起動でコスト削減）。成果物：`manifests/history-server.yaml` or 起動手順。
- [ ] **P6.2 Cloud Logging 確認** — driver/executor の stdout/stderr を確認できる状態。
- [ ] **P6.3（任意）メトリクス** — Operator の Prometheus メトリクス + Grafana。
- [ ] **P6.4 コストガードレール** — spark プールの 0 台縮退を確認。**teardown スクリプト**（クラスタ削除）と再作成手順。成果物：`infra/teardown.sh`。
  - **DoD（Phase6 完了）**：アイドル時 spark ノード 0／teardown で課金停止を確認。

---

## Phase 7（任意）— HDFS/YARN 学習モジュール（付録 G） ← 独立

- [ ] **P7.1 最小 HDFS + MapReduce** — NameNode1 + DataNode1〜2 の StatefulSet、`hdfs dfs -put`、wordcount を YARN で1本。終わったら削除。成果物：`manifests/hdfs/`。DoD：put/get と MapReduce が1本通る。

---

## 横断：テスト戦略（§12）

- [ ] **TT.1 単体** — 変換ロジックを `local[*]` + pytest（小さな固定入力）。← P2.5
- [ ] **TT.2 データ品質** — `validate_dq` の閾値テスト。← P3.1
- [ ] **TT.3 冪等性** — 同 run_date 2回で一致（P3.3 と同一）。
- [ ] **TT.4 スキュー** — 偏りデータで before/after（P5 と同一）。
- [ ] **TT.5 結合（小）** — raw（小）→ curated まで一気通貫を小データで。← P3

---

## クリティカルパス（最短で「動くバッチ1本」）

P0 → P1（疎通）→ P2（staging まで）→ P3（DQ+publish+冪等）→ P4（Airflow 日次）。
Phase 5/6/7 は P4 完了後に随時。コスト面は P6.4 の teardown を**着手直後から**使えるよう先に用意しておくと安全。
