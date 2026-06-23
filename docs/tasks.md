# 実装タスク分割 — spark-on-k8s

`docs/Design Doc.md` を実装に落とすための詳細タスク。各タスクに **成果物（ファイル/コマンド）** と **完了条件（DoD）** を付ける。フェーズは設計 §13 に対応し、より細かく分解している。

凡例：`[ ]` 未着手 / `[~]` 進行中 / `[x]` 完了。ID は `P<phase>.<n>`。依存は「← Px.y」で示す。

設計の不変条件（§6.1, §7, §8, §15）は全タスク共通の前提：
- `run_date` は**処理対象データの論理日付**（実行日ではない）。raw は `dt=YYYY-MM-DD/` の当日スライスのみ読む。
- 出力本線は **staging → DQ → publish ＋ `_SUCCESS`**。
- 冪等：同じ `run_date` の再実行で結果が一意。
- バージョンは固定（Spark 3.5.x / Java 17 / `gcs-connector-hadoop3` を具体版で）。`-latest` 禁止。
- 認証は Workload Identity（ADC）のみ。鍵をコミットしない。

## IaC 方針（レイヤ分割）

「作る→使う→消す→また作る」を繰り返す前提（§14）なので、**GCP 資源は Terraform で state 管理**し、`terraform destroy` で**消し残しゼロ**を担保する。クラスタ内リソースは TF に含めない（クラスタ destroy 時の破棄順序ハマりを回避）。

| レイヤ | ツール | 対象 |
| :---- | :---- | :---- |
| GCP 資源 | **Terraform**（`google` provider） | API 有効化・GCS バケット・GKE クラスタ・ノードプール・GSA・IAM/WI バインド・Artifact Registry |
| クラスタ内 | **Helm** | Spark Operator・Airflow |
| k8s マニフェスト | **kubectl/YAML** | SparkApplication・RBAC・KSA（＋WI アノテーション） |

state は個人用途ならローカルでも可。machine をまたぐなら GCS backend を推奨。Workload Identity は **GCP 側の IAM バインド＝TF**、**KSA 作成＋アノテーション＝kubectl** に分割する。

---

## Phase 0 — 前提・リポジトリ整備

- [ ] **P0.1 GCP プロジェクト初期化**
  - 手順：プロジェクト選択/作成、課金紐付け、最初の API（`cloudresourcemanager` 等）だけ手動有効化（以降の API は P1 で TF `google_project_service` に寄せる）、**予算アラート設定**。
  - 成果物：`infra/README.md`（手順）
  - DoD：`gcloud config get-value project` が対象プロジェクト。予算アラート有効。
- [ ] **P0.2 ローカルツール準備**
  - 手順：`gcloud` / `kubectl` / `helm` / `docker` / `gh` / **`terraform`** のインストール・バージョン確認、`gcloud auth login` と ADC（`gcloud auth application-default login`）。
  - DoD：各 `--version` 表示、ADC 取得済み。
- [ ] **P0.3 リポジトリ scaffolding**
  - 手順：`jobs/` `dags/` `manifests/` `infra/terraform/` `tests/` を作成。`.gitignore`（`*-key.json` / `.env` / `kubeconfig` / `__pycache__/` / `.venv/` / **`.terraform/` / `*.tfstate*` / `*.tfvars`**）。`infra/versions.env` にピン版（Spark, GCS connector, Operator chart, Airflow chart, provider）。
  - 成果物：ディレクトリ群、`.gitignore`、`infra/versions.env`
  - DoD：scaffolding がコミットされている。
- [ ] **P0.4 Terraform 初期化** ← P0.2,P0.3
  - 手順：`infra/terraform/{versions.tf, providers.tf, variables.tf, terraform.tfvars.example}`。`versions.tf` で `google` provider をピン。（任意）state 用 GCS バケットを作成し `backend "gcs"` を設定。`terraform init`。
  - 成果物：`infra/terraform/*.tf`
  - DoD：`terraform init` 成功・`terraform plan` がクリーン（空 or 期待差分）。

---

## Phase 1 — 基盤と疎通（GKE + GCS + Operator + SparkPi） ← P0

GCP 資源は TF（`infra/terraform/`）に集約。クラスタ内は Helm/kubectl。

- [ ] **P1.1 TF 変数** — `variables.tf` / `terraform.tfvars`（`project_id`, `region=asia-northeast1`, `lake_bucket`, `cluster_name`, machine types, `spark_max_nodes` 等）。
- [ ] **P1.2 API 有効化（TF）** — `google_project_service`（`container`, `artifactregistry`, `storage`, `iam`）。DoD：`apply` で有効化。
- [ ] **P1.3 GCS バケット（TF）** ← P1.1 — `google_storage_bucket`（uniform access, `location=region`）。3層はプレフィックス運用なのでバケットは1つ。DoD：`apply` でバケット作成。
- [ ] **P1.4 GKE クラスタ（TF）** ← P1.2 — `google_container_cluster`（Standard, regional, `release_channel=REGULAR`, **`workload_identity_config`**, default node pool は削除し別管理, system プール `e2-standard-4`×1, on-demand）。成果物：`infra/terraform/gke.tf`。DoD：`kubectl get nodes` 応答。
- [ ] **P1.5 Spark Spot ノードプール（TF）** ← P1.4 — `google_container_node_pool`（`spot=true`, autoscaling `min=0`/`max=N`, `n2-standard-8`, label `workload=spark`, taint `workload=spark:NoSchedule`）。DoD：プール作成・アイドル0台。
- [ ] **P1.6 GSA + IAM/WI（GCP 側・TF）** ← P1.3 — `google_service_account`(`spark-gsa`)、`google_storage_bucket_iam_member`(`roles/storage.objectAdmin` on bucket)、`google_service_account_iam_member`(`roles/iam.workloadIdentityUser`, member=`serviceAccount:${project}.svc.id.goog[spark-jobs/spark]`)。成果物：`infra/terraform/iam.tf`。DoD：`apply` 成功。
- [ ] **P1.7 Artifact Registry（TF）** ← P1.2 — `google_artifact_registry_repository`（docker, `location=region`, 名前 `spark`）。DoD：repo 作成。
- [ ] **P1.8 namespace（kubectl）** — `spark-jobs` / `spark-operator` / `airflow`。
- [ ] **P1.9 KSA + WI アノテーション（k8s 側・kubectl）** ← P1.6,P1.8 — KSA `spark`(ns: spark-jobs)作成＋ `iam.gke.io/gcp-service-account=spark-gsa@...` アノテーション。成果物：`manifests/ksa.yaml`。DoD：テスト Pod が ADC で GCS list 成功（403 が出ない）。
- [ ] **P1.10 RBAC（kubectl）** ← P1.8 — `spark-role`/`spark-rb`（pods/services/configmaps/pvc 等）。成果物：`manifests/spark-rbac.yaml`。
- [ ] **P1.11 Spark Operator（Helm）** ← P1.8 — chart v2.x, `spark.jobNamespaces={spark-jobs}`, `webhook.enable=true`。成果物：`infra/operator.sh`。DoD：operator Pod Running。
- [ ] **P1.12 SparkPi スモークテスト（kubectl）** ← P1.10,P1.11 — サンプル `SparkApplication`。成果物：`manifests/spark-pi.yaml`。**DoD（Phase1 完了）**：spark-pi `Completed`、ジョブ後に spark プールが 0 台へ縮退。

---

## Phase 2 — 自前イメージ + 変換1本（raw → staging Parquet） ← P1

- [ ] **P2.1 自前イメージ** ← P1.7 — `FROM apache/spark:3.5.x`（ピン）、**ピン版 `gcs-connector-hadoop3-<x.y.z>-shaded.jar`** を `/opt/spark/jars/` へ、`COPY jobs/`。build & push（AR repo は P1.7 で TF 作成済み）。成果物：`Dockerfile`, `infra/image.sh`。DoD：イメージが AR に push 済み。
- [ ] **P2.2 データセット選定 & スキーマ定義** — Kaggle データ確定、**明示 `StructType`**（`inferSchema` 不使用）、ビジネスキー特定。成果物：`jobs/schema.py`。
- [ ] **P2.3 取り込み** ← P2.2 — CSV を `raw/<dataset>/dt=YYYY-MM-DD/` に配置。成果物：`infra/ingest.sh` or `jobs/ingest.py`。DoD：raw に当日スライス存在。
- [ ] **P2.4 `aggregate.py` 実装** ← P2.2 — 引数 `--run-date`、`raw/.../dt={run_date}/` を明示スキーマで読む、`dropDuplicates([business_key])`、`groupBy` 集計、`withColumn("run_date", lit(run_date))`、`coalesce(N)`、**`staging/<job>/` に `partitionBy("run_date")` で書く**（curated には書かない）。成果物：`jobs/aggregate.py`。
- [ ] **P2.5 SparkApplication マニフェスト** ← P2.1,P2.4 — ピン image、`sparkConf`（AQE 一式、`partitionOverwriteMode=dynamic`、`eventLog.dir=gs://.../spark-events/`、GCS コネクタ + ADC）、**driver は system プールへ明示配置（nodeSelector/affinity）**、executor は spot（nodeSelector + tolerations）、`restartPolicy`、`timeToLiveSeconds`。成果物：`manifests/spark_application.yaml`。
- [ ] **P2.6 手動実行 & 確認** ← P2.3,P2.5 — apply して staging に Parquet 出力、History Server でステージ確認。**DoD（Phase2 完了）**：staging に正しい Parquet／SHS でステージが見える。

---

## Phase 3 — DQ + publish + 冪等性（本書の核） ← P2

- [ ] **P3.1 `validate_dq` 実装** — staging の出力を検証（行数 > 0、キー NULL 率 < 閾値、スキーマ一致）。閾値割れで失敗（publish させない）。成果物：`jobs/validate_dq.py`。
- [ ] **P3.2 `publish` 実装** ← P3.1 — `staging/<job>/<ds>/run_date=<ds>/` を `curated/<table>/run_date=<ds>/` に昇格し、**`_SUCCESS` センチネル**を設置。同一パーティション上書きで冪等。成果物：`jobs/publish.py`。
- [ ] **P3.3 冪等性テスト** ← P3.2 — 同じ `run_date` を 2 回流し、出力（件数/ハッシュ）が完全一致。成果物：`tests/test_idempotency.py`。
- [ ] **P3.4 読み手の `_SUCCESS` ゲート** — curated を読む側は `_SUCCESS` のあるパーティションのみ信頼する規約を明文化＋ヘルパ。
  - **DoD（Phase3 完了）**：同じ日付2回で結果一意／DQ 失敗時に curated が無傷。

---

## Phase 4 — オーケストレーション（Airflow） ← P3

- [ ] **P4.1 Airflow 導入（Helm）** — official chart、`executor=KubernetesExecutor`、DAG 配布（git-sync or イメージ同梱）。provider `apache-airflow-providers-cncf-kubernetes` を**バージョン固定**。成果物：`infra/airflow.sh`, `values-airflow.yaml`。DoD：Web UI 到達。
- [ ] **P4.2 K8s 接続** ← P4.1 — `kubernetes_default` 接続（in-cluster）。DoD：Airflow から spark-jobs に apply 可能。
- [ ] **P4.3 DAG `kaggle_batch`** ← P4.2 — タスク連鎖 `ingest_check → spark_aggregate(SparkKubernetesOperator) → validate_dq → publish`。`{{ ds }}` 伝播、`retries=2`、`catchup=True`、`max_active_runs=1`、**同名 SparkApplication の冪等 apply（apply 前 delete）**、完了待ち（sensor/deferrable）を provider 版に合わせて実装。成果物：`dags/kaggle_batch.py`, `dags/spark_application.yaml`。
- [ ] **P4.4 バックフィル検証** ← P4.3 — 過去日の範囲を流し、日付別パーティションが正しく作られることを確認。
  - **DoD（Phase4 完了）**：同じ日付2回で一意／バックフィルが動く。

---

## Phase 5 — スキュー再現と調整 ← P2（最低限）/ P4（理想）

- [x] **P5.1 偏りデータ生成** — 特定キー（`hot`）に偏ったデータを raw に投入。成果物：`jobs/gen_skewed.py`。
- [~] **P5.2 ベースライン計測** ← P5.1 — 対策前の wall-clock とタスク分布（max/median 比）を SHS で記録。ハーネス実装済み（`infra/run-skew.sh` が baseline `agg-salt1` を実行）。実測は SHS で（`docs/skew-experiment.md`）。
- [~] **P5.3 緩和と比較** ← P5.2 — salting（`aggregate.py --salt N` の二段集計。結果不変はユニットテスト `test_transform_salted_matches_unsalted` で担保）、AQE 一式はマニフェストで有効。`infra/run-skew.sh` が `agg-salt16` を実行し比較可能に。
  - **DoD（Phase5 完了）**：対策前後の wall-clock 改善を SHS で確認。手順 `docs/skew-experiment.md`。実測は実行時タスク。

---

## Phase 6 — 観測性とコスト ← P2

- [x] **P6.1 History Server** — `gs://.../spark-events/` を参照（常設せず必要時起動でコスト削減）。成果物：`manifests/history-server.yaml`, `infra/history-server.sh`（`up`/`delete`）、`docs/observability.md`。spark KSA で WI 経由 GCS 読み取り。
- [x] **P6.2 Cloud Logging 確認** — driver/executor の stdout/stderr を確認する手順を文書化。成果物：`docs/observability.md`（`gcloud logging read` / `kubectl logs`）。
- [~] **P6.3（任意）メトリクス** — Operator の Prometheus メトリクス + GMP/Grafana。任意のため文書化のみ（`docs/observability.md`）。
- [x] **P6.4 コストガードレール / teardown** — spark プールの 0 台縮退、`terraform destroy` での消し残しゼロ、in-cluster の `helm uninstall` 順序、データ温存の部分 destroy を文書化。成果物：`infra/teardown.md`。
  - **DoD（Phase6 完了）**：アイドル時 spark ノード 0／`terraform destroy` 後に課金リソースが残らない（`gcloud ... list` で確認）。手順 `infra/teardown.md`。

---

## Phase 7（任意）— HDFS/YARN 学習モジュール（付録 G） ← 独立

- [ ] **P7.1 最小 HDFS + MapReduce** — NameNode1 + DataNode1〜2 の StatefulSet、`hdfs dfs -put`、wordcount を YARN で1本。終わったら削除。成果物：`manifests/hdfs/`。DoD：put/get と MapReduce が1本通る。

---

## 横断：テスト戦略（§12）

- [ ] **TT.1 単体** — 変換ロジックを `local[*]` + pytest（小さな固定入力）。← P2.4
- [ ] **TT.2 データ品質** — `validate_dq` の閾値テスト。← P3.1
- [ ] **TT.3 冪等性** — 同 run_date 2回で一致（P3.3 と同一）。
- [ ] **TT.4 スキュー** — 偏りデータで before/after（P5 と同一）。
- [ ] **TT.5 結合（小）** — raw（小）→ curated まで一気通貫を小データで。← P3

---

## クリティカルパス（最短で「動くバッチ1本」）

P0（TF 初期化含む）→ P1（`terraform apply` で GCP 資源 + Operator 疎通）→ P2（staging まで）→ P3（DQ+publish+冪等）→ P4（Airflow 日次）。
Phase 5/6/7 は P4 完了後に随時。`terraform destroy`（P6.4）は**着手直後から**使えるよう、P1 完了時点で一度 destroy→apply を試して再現性を確認しておくと安全。
