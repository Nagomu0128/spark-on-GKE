# Design Doc — GKE 上に Hadoop/Spark バッチ処理パイプラインを構築する

| 項目 | 内容 |
| :---- | :---- |
| 著者 | 吉田 和哉 |
| ステータス | Draft（実装着手前） |
| 対象 | 個人学習プロジェクト（LINEヤフー SWE-4-64「Hadoop/Spark × Kubernetes による大規模バッチ処理基盤」を想定した自主検証） |
| 主軸構成 | **Spark on Kubernetes（GKE）+ GCS**、オーケストレーションは **自前 Airflow**、実行は **Spark Operator（SparkApplication CRD）** |
| 最終ゴール | Kaggle データを GCS に置き、GKE 上の Spark で分散集計し、結果を Parquet で GCS に書き出すバッチを、Airflow で日次・冪等・スケール可能に回す |

補足：本書は「設計を先に言語化してから実装する」方針で書いている。各章に **選定理由 / トレードオフ** を残し、後から「なぜこの作りなのか」を説明できる状態にすることを目的とする。

---

## 1\. 背景と目的

### 1.1 背景

- 募集要件にある Hadoop / Spark / Airflow を、クラウドのマネージドに頼らず **自分で立てて動かす**ことで、大規模バッチ基盤の挙動を体で理解したい。  
- 小さく動かすだけでも現れる「分散ならではの難所」（**二重実行 → 冪等性**、**データの偏り → スキュー**）を、実機で再現・対処して学ぶ。

### 1.2 目的（このプロジェクトのゴール）

1. GKE 上に Spark on Kubernetes を構築し、GCS を入出力ストレージとした **動くバッチ**を1本通す。  
2. Airflow で **日次スケジュール / リトライ / バックフィル**を回す。  
3. **冪等性**（再実行しても結果が壊れない）を設計に組み込む。  
4. **データスキュー**を意図的に再現し、検知・緩和まで一連を体験する。  
5. すべてを **再現可能（IaC・コマンド化）** にし、使わない時は **スケール 0 / クラスタ削除**でコストを抑える。

---

## 2\. ゴール / 非ゴール

### ゴール

- 単一データセット（Kaggle の実データ）に対する **バッチ集計**パイプライン。  
- GCS の 3 層レイアウト（raw / staging / curated）と Parquet 出力。  
- Spark Operator による宣言的ジョブ投入、Airflow による起動・依存管理。  
- 冪等性・スキュー対策・観測性・テストの「型」を一通り通す。

### 非ゴール（今回はやらない）

- ストリーミング処理（Structured Streaming）。バッチに集中する。  
- 本物の HDFS \+ YARN を本番運用すること（→ 学習用の任意モジュールとして付録 G に最小構成だけ示す）。  
- マルチテナント運用、SLA、本格的なデータカタログ／リネージ基盤。  
- 機械学習（MLlib）パイプライン。

---

## 3\. 要件

### 3.1 機能要件

- **取り込み**：Kaggle の CSV を GCS `raw/` に配置できる。  
- **変換**：Spark でクレンジング＋キー単位の集計（`groupBy` → `count` / `sum`）。  
- **出力**：列指向 Parquet（snappy 圧縮）を GCS `curated/` に、論理日付でパーティション出力。  
- **スケジュール**：日次実行。任意日付でのバックフィルが可能。  
- **検証**：出力の行数・NULL 率などの簡易データ品質（DQ）チェック。

### 3.2 非機能要件

| 観点 | 要件 | 備考 |
| :---- | :---- | :---- |
| 規模 | まずは数百 MB〜数 GB。Executor 数で水平スケールできること | JD は数十 GB〜数 TB／日次 3 万件以上。設計はその縮小版として作る |
| 冪等性 | 同じ論理日付で何度再実行しても結果が一意になる | 本書の核（§8） |
| スキュー耐性 | 偏りを検知でき、緩和手段を適用できる | 本書の核（§9） |
| 再現性 | クラスタ・権限・ジョブをコマンド/YAML で再構築できる | IaC（gcloud/helm/manifest） |
| コスト | アイドル時は Spark ノードを 0 台、必要時のみ起動 | Spot \+ Autoscaling（min 0） |
| 観測性 | ジョブの成否・実行時間・タスク偏りが見える | Spark History Server \+ Cloud Logging |
| セキュリティ | 静的キーを使わずに GCS へアクセス | Workload Identity（§10） |

---

## 4\. アーキテクチャ決定（ADR）：Hadoop(HDFS+YARN) vs Spark on K8s \+ GCS

「Hadoop を GKE で」をどう解釈するかが本プロジェクト最大の技術選定。結論は **Spark on Kubernetes \+ GCS**。

| 観点 | A. 本物の HDFS \+ YARN を K8s に載せる | B. Spark on K8s \+ GCS（採用） |
| :---- | :---- | :---- |
| スケジューラ | YARN（K8s と二重管理） | K8s スケジューラに一本化 |
| ストレージ | HDFS（DataNode が状態を持つ＝PV 必須・運用重い） | GCS（計算と分離・スケール 0 が容易） |
| クラウド適合 | 低い（オンプレ前提の設計） | 高い（"× Kubernetes" の現代的な型） |
| Hadoop 体験 | HDFS/YARN を直に触れる | Hadoop **クライアントライブラリ**（FileFormat・GCS コネクタ）経由で Hadoop エコシステムは使う |
| 構築コスト | 高い | 低い〜中 |
| JD/デッキとの一致 | △ | ◎ |

### 決定

- **B を採用**。理由：(1) JD の「× Kubernetes」と現代的なクラウド設計に一致、(2) ストレージを GCS に分離することでクラスタをスケール 0 にでき、学習コストを最小化できる、(3) Spark は内部で Hadoop のクライアントライブラリを使うため、Parquet・GCS コネクタなど **Hadoop エコシステムの中核**は B でも学べる。  
- **HDFS/YARN を直に触る学習欲**は、本筋を汚さないために **付録 G の任意モジュール**（最小 HDFS \+ MapReduce を一度動かす）として切り出す。

トレードオフ：B では「HDFS の運用（レプリケーション、NameNode 等）」は体験できない。そこは付録 G で補完するか、別途 Dataproc 等で触る。

---

## 5\. 全体アーキテクチャ

                ┌────────────────────────── GCP Project ──────────────────────────┐

                │                                                                  │

   Kaggle CSV ──┼──►  GCS  gs://\<lake\>/raw/        (生データ)                       │

                │        gs://\<lake\>/staging/      (中間・原子的 publish 用)         │

                │        gs://\<lake\>/curated/      (Parquet・最終)                  │

                │        gs://\<lake\>/spark-events/ (History Server 用イベントログ)   │

                │            ▲          ▲                                           │

                │   read/write│          │ events                                   │

                │  ┌──────────┴──────────┴──────────────── GKE (Standard) ───────┐  │

                │  │  ns: airflow         ns: spark-operator   ns: spark-jobs     │  │

                │  │  ┌───────────┐       ┌──────────────┐     ┌───────────────┐  │  │

                │  │  │ Airflow   │ apply │ Spark        │watch│ SparkApplication│ │  │

                │  │  │ scheduler │──CRD─►│ Operator     │────►│  Driver Pod    │  │  │

                │  │  │ webserver │       │ (controller  │     │   └─ Executor  │  │  │

                │  │  │ workers   │◄─状態─│  \+ webhook)  │     │      Pods ×N   │  │  │

                │  │  └───────────┘       └──────────────┘     └───────────────┘  │  │

                │  │  node-pool: system (on-demand)   node-pool: spark (Spot, min0)│  │

                │  └───────────────────────────────────────────────────────────┘  │

                │   認証: Workload Identity (KSA ⇄ GSA)  /  ログ: Cloud Logging       │

                └──────────────────────────────────────────────────────────────────┘

### データフロー

1. Kaggle の CSV を `raw/` にアップロード（手動 or 取り込みタスク）。  
2. Airflow DAG が日次起動 → `SparkKubernetesOperator` が **SparkApplication** を apply。  
3. Spark Operator が `spark-submit` を代行し、Driver/Executor Pod を **Spot ノードプール**に展開。  
4. Spark が `raw/` を読み、集計し、`curated/` に **論理日付でパーティション**した Parquet を書く（**動的パーティション上書き**で冪等）。  
5. DQ チェックタスクが行数・NULL 率を検証。OK なら成功。  
6. イベントログは `spark-events/` に出力 → History Server で後から閲覧。

---

## 6\. コンポーネント設計

### 6.1 ストレージ（GCS）

- バケットは原則 1 つ（`gs://<project>-datalake`）、プレフィックスで層を分ける。  
  - `raw/<dataset>/dt=YYYY-MM-DD/...`：投入そのまま（不変）。**論理日付 `dt` でパーティション**し、ジョブはその日のスライスだけを読む（§8）。  
  - `staging/<job>/<run_date>/...`：書き込み途中・検証前の置き場（原子的 publish 用）。  
  - `curated/<table>/run_date=YYYY-MM-DD/...`：最終 Parquet（Hive 形式パーティション）。  
  - `spark-events/`：Spark イベントログ。  
- **パーティション**：入力 `raw/` は `dt`（論理日付）で、出力 `curated/` は同じ論理日付の `run_date` でパーティション。**`run_date` は「処理対象データの論理日付」であって「ジョブを実行した日」ではない**。再実行・バックフィルの単位と一致させる。  
- **ファイルサイズ**：小ファイル乱立を避けるため、書き込み前に `repartition`/`coalesce` で 128〜256MB/ファイルを狙う。  
- **圧縮**：Parquet \+ snappy（読み取り速度と圧縮率のバランス）。  
- **選定理由**：計算とストレージを分離することで、クラスタを落としてもデータは残り、スケール 0 運用ができる。

### 6.2 実行基盤（GKE）

- **クラスタ**：Standard、リージョナル（例 `asia-northeast1`）、Release channel \= regular、**Workload Identity 有効**。  
  - Autopilot ではなく Standard を選ぶ理由：Spark の shuffle 用に **ローカル SSD / ephemeral-storage** やノードプール構成（Spot・taint）を細かく制御したいため。  
- **ノードプール**：  
  - `system`（on-demand, 1〜2 台, 例 `e2-standard-4`）：Airflow・Operator・Spark **Driver** を置く（安定性重視。Driver が落ちるとジョブ全体が死ぬため Spot に置かない）。  
  - `spark`（**Spot**, autoscaling **min 0**〜max N, 例 `n2-standard-8`）：Spark **Executor** 専用。`workload=spark` の label と taint を付け、Executor だけスケジュールする。  
- **shuffle**：大きめジョブではローカル SSD を付けて shuffle/spill を高速化（`--ephemeral-storage-local-ssd`）。まずは無しで開始可。  
- **namespace**：`airflow` / `spark-operator` / `spark-jobs` の 3 つに分離。  
- **選定理由**：Driver=安定・Executor=Spot で「壊れにくさ」と「コスト」を両立。min 0 によりアイドルコストをほぼゼロにできる。

### 6.3 Spark 実行方式（Spark Operator）

- **Kubeflow Spark Operator** を Helm で導入し、**SparkApplication** / **ScheduledSparkApplication** CRD でジョブを宣言。  
  - Helm repo: `https://kubeflow.github.io/spark-operator`、chart `spark-operator/spark-operator`、CRD `apiVersion: sparkoperator.k8s.io/v1beta2`。  
  - Operator が `spark-submit` を代行し、リトライ（`onFailureRetries`）・再起動ポリシー・Prometheus メトリクスを提供。  
- **素の spark-submit ではなく Operator を選ぶ理由**：ジョブを \*\*宣言的（YAML）\*\*に管理でき、Airflow からは「YAML を apply するだけ」で済む。リトライ/メトリクス/Pod カスタマイズ（volume・configmap マウント）が標準で付く。  
- **コンテナイメージ**：`apache/spark:3.5.x` をベースに、**GCS コネクタ jar** とジョブ（PySpark）を焼き込んだ自前イメージを Artifact Registry に置く（付録 C）。  
  - GCS コネクタを `spark.jars.packages` で実行時 DL するより、**イメージに焼き込む**方が K8s 上で安定。  
- **大規模時の注意**：Operator は SparkApplication ごとに Service を作るため、大量投入時は完了オブジェクトの GC（`timeToLiveSeconds` 設定）が必須。3 万件規模を見据える場合の論点としてメモ（今回は少数なので `timeToLiveSeconds` を付けておく程度）。

### 6.4 オーケストレーション（自前 Airflow on GKE）

- **Apache Airflow を Helm（official chart）で GKE に導入**。Executor は KubernetesExecutor（タスクごとに Pod、スケールしやすい）。  
- Spark ジョブの起動は **`SparkKubernetesOperator`**（`apache-airflow-providers-cncf-kubernetes`）で SparkApplication を apply。完了待ちは provider のバージョンにより挙動が異なるため、**`SparkKubernetesSensor` の要否はインストールした provider 版で確認**する。  
- **Cloud Composer ではなく自前**を選ぶ理由：JD の学習目的（Airflow の運用も自分で）に合わせる。トレードオフとして運用負荷は上がるが、学習効果が高い。  
- **DAG 設計**：  
  - `schedule="@daily"`。**バックフィルは `catchup=True`（`start_date` を妥当な過去日に設定）で自動実行**（§3.1）。意図しない大量実行は `max_active_runs` で抑制。  
  - **同名 SparkApplication の衝突回避**：名前を `ds` でキーにするためリトライ・同日再実行で既存と衝突（AlreadyExists）しうる。apply 前に既存を delete してから作る（冪等 apply）か reconcile する provider 版を使い、`timeToLiveSeconds`（付録 D）GC 前の再実行で詰まらないよう operator の削除挙動を実機確認。  
  - **多重起動の抑止**：Airflow 側にも `concurrencyPolicy: Forbid` 相当を置く（`max_active_runs=1`）。  
  - 論理日付 `{{ ds }}` を SparkApplication の引数・出力パーティションに伝播。  
  - リトライ：`retries=2`, `retry_delay=5min`（指数バックオフ可）。  
  - **タスクを冪等に**：同じ `ds` での再実行が安全（§8）であることが前提。

### 6.5 （任意）メタストア

- 最初は **ファイルベース Parquet のみ**で十分。SQL/テーブル管理が欲しくなったら、**Hive Metastore（Cloud SQL バック）** または BigLake/BigQuery 外部テーブルを拡張として追加（今回は非ゴール）。

---

## 7\. データパイプライン設計（Airflow DAG）

DAG: `kaggle_batch`（日次）

| 順 | タスク | 役割 | 失敗時 |
| :---- | :---- | :---- | :---- |
| 1 | `ingest_check` | `raw/<dataset>/dt={{ ds }}/` に当日対象データが存在するか確認（無ければ取り込み or skip） | リトライ |
| 2 | `spark_aggregate` | SparkApplication を apply し、集計 Parquet を **`staging/<job>/{{ ds }}/` に出力**（curated には直接書かない） | リトライ（冪等なので安全） |
| 3 | `validate_dq` | **`staging/` の出力**を検証（行数 \> 0、キー NULL 率 \< 閾値 など） | 失敗で停止（**publish しない**→ curated は無傷） |
| 4 | `publish` | 検証済み出力を `curated/<table>/run_date={{ ds }}/` に昇格し `_SUCCESS` を置く | リトライ |

- **方式の選択（標準は staging→DQ→publish）**：  
  - **標準形** \= Spark は `staging/` に書き、`validate_dq` が staging を検証、合格後に `publish` が `curated/` へ昇格して `_SUCCESS` を置く（**DQ を公開の前に置く**）。curated を読む側は `_SUCCESS` のあるパーティションだけ信頼する。  
  - **シンプル版（ローカル/小データ限定）** \= Spark が直接 `curated/.../run_date={{ ds }}/` に動的パーティション上書き（§8）。DQ が公開後に走り curated に不正データが残りうるため、GKE 本線では使わない。

---

## 8\. 冪等性設計（本書の核 ①）

**目的**：リトライ・バックフィル・二重起動があっても、`curated/` の結果が常に一意になること。

### 採用する仕組み

1. **論理日付パーティション \+ 動的パーティション上書き**  
   - 出力は `curated/<table>/run_date=YYYY-MM-DD/`。  
   - Spark 設定 `spark.sql.sources.partitionOverwriteMode=dynamic` \+ `df.write.mode("overwrite").partitionBy("run_date")` により、**対象 run\_date のパーティションだけ**を置き換える（他日付は無傷）。→ 同じ日を何度流しても結果が一意。  
   - **GCS 上の注意（重要）**：GCS の rename は「コピー＋削除」で**非原子的**。動的上書きは「一時ディレクトリ→対象パーティション削除→rename」で実装されるため、rename 途中の失敗で**対象パーティションが「削除済み・未書き込み」（その日のデータ消失）**になりうる。よって**動的上書き単独は冪等性の保証としない**。GKE 本線は下記 3 の staging→publish ＋ `_SUCCESS` とし、動的上書きはシンプル版（ローカル/小データ）に限定する。  
2. **ビジネスキーでの重複排除**  
   - 取り込み段で重複が混じり得る場合、集計前に `dropDuplicates([business_key])`。または `groupBy` 自体が重複に強い形にする。  
3. **staging→publish ＋ `_SUCCESS` センチネル（GKE 上の本線）**  
   - `staging/<job>/<run_date>/` に書き切り、DQ 合格後に `curated/` へ昇格（§7）。途中失敗を最終層に出さない。  
   - 昇格完了で `curated/<table>/run_date=<ds>/_SUCCESS` を置き、**読み手は `_SUCCESS` のあるパーティションだけ信頼**する。これにより publish が非原子でも「中途半端を読ませない」を担保。`_SUCCESS` の有無で完了判定し、二重処理もスキップできる。  
4. **Airflow タスクの再実行安全性**  
   - すべてのタスクは「同じ `ds` で再実行しても副作用が増えない」ように書く（追記ではなく上書き／存在チェック）。

### 設計原則

- 配信・実行は現実には **at-least-once**（リトライ前提）になる。だから **処理側を冪等**にして、結果として **exactly-once 相当**に見せる。失敗を止めにいくのではなく「何度走っても安全」にするのが正攻法。

---

## 9\. データスキュー対策（本書の核 ②）

**目的**：特定キーへの偏りで 1 タスクだけが極端に遅くなり、全体（wall-clock）がそれに律速される問題を、検知し緩和する。

### 検知

- **Spark History Server / UI** のステージ画面で、タスクごとの実行時間・入力サイズの分布を見る。1 つだけ突出していれば偏りを疑う。  
- メトリクス（タスクの max/median 比）で機械的に気づける状態にしておく。

### 緩和（軽い順に試す）

1. **AQE を有効化**：`spark.sql.adaptive.enabled=true`、`spark.sql.adaptive.skewJoin.enabled=true`、`spark.sql.adaptive.coalescePartitions.enabled=true`。  
2. **シャッフルパーティション調整**：`spark.sql.shuffle.partitions` をデータ量に合わせて増減。  
3. **salting**：偏ったキーに乱数サフィックスを付けて分散 → 集計後に統合。  
4. **broadcast join**：小さい次元表は `broadcast()` で配り、シャッフル自体を避ける。  
5. **repartition**：書き込み前に `repartition(n, key)` で粒度を整える（小ファイル対策も兼ねる）。

### 学習としての再現

- 偏ったキーを多く含むデータを作って **意図的にスキューを発生**させ、対策前後で wall-clock を比較する（デッキ 9 枚目の体験の裏取り）。  
- 正直、深いチューニングまでは初学者には難しい。まずは「**偏りは必ず出る → 気づける状態にする**」を第一歩に置く。

---

## 10\. セキュリティ

- **Workload Identity** を使い、**静的なサービスアカウントキーを一切使わない**。  
  - GCP サービスアカウント（GSA）に GCS バケットの最小権限（`roles/storage.objectAdmin` をバケット限定）を付与。  
  - Spark ジョブ用 K8s サービスアカウント（KSA）を GSA にバインド（`roles/iam.workloadIdentityUser`）し、KSA にアノテーション。  
  - Driver/Executor Pod はこの KSA で動き、ADC（Application Default Credentials）経由で GCS にアクセス。  
- **最小権限**：層ごとに権限を分けられるなら raw=読み取り、curated=書き込み等に分離。  
- **シークレット**：必要なものは Secret Manager / K8s Secret に。リポジトリに鍵を置かない。  
- **RBAC**：Spark Driver が Executor Pod を作れるよう、`spark-jobs` namespace に Role/RoleBinding（pods/services/configmaps の作成権限）。Operator の Helm が作る SA を使うのが簡単。

---

## 11\. 観測性

- **Spark History Server**：`spark.eventLog.enabled=true`、`spark.eventLog.dir=gs://<lake>/spark-events/` に出力 → History Server（GKE 上 or ローカル）で後から DAG/ステージ/タスクを分析。スキュー検知の主役。  
- **ログ**：GKE → Cloud Logging に集約。Driver/Executor の stdout/stderr を確認。  
- **メトリクス**：Spark Operator が Prometheus メトリクスを公開（Helm で有効化）。Grafana ダッシュボードでジョブ処理レート・キュー深さ等を可視化（任意）。  
- **Airflow**：Web UI でタスクの成否・所要時間・リトライ履歴。  
- **DQ**：`validate_dq` の結果をログ/メトリクス化し、閾値割れで失敗させる。

---

## 12\. テスト戦略

| レベル | 内容 | 手段 |
| :---- | :---- | :---- |
| 単体 | 変換ロジック（集計・クレンジング）の正しさ | ローカル Spark（local\[\*\]）+ pytest、小さな固定入力 |
| 結合（小） | raw（小データ）→ curated まで一気通貫 | 小サイズで GKE 上 or ローカルで実行 |
| データ品質 | 行数 \> 0、キー NULL 率、想定スキーマ | `validate_dq` タスク |
| 冪等性 | 同じ run\_date を **2 回**流して出力が完全一致 | ハッシュ/件数比較 |
| スキュー | 偏りデータで 1 タスク律速を再現 → 対策で改善 | Spark UI で before/after |

---

## 13\. 段階的構築計画（Phase）

各フェーズに「完了条件（DoD）」を置き、小さく積み上げる。

- **Phase 0｜前提**：GCP プロジェクト、課金、`gcloud`/`kubectl`/`helm` 準備、Artifact Registry 作成、予算アラート設定。  
  - DoD：`gcloud` でプロジェクトに接続でき、予算アラートが有効。  
- **Phase 1｜基盤と疎通**：GKE 作成（WI 有効）、ノードプール（system / spark-spot）、GCS バケット作成、Spark Operator 導入、**SparkPi のサンプル**を実行。  
  - DoD：SparkApplication（spark-pi）が Completed になる。  
- **Phase 2｜変換 1 本**：自前イメージ（Spark \+ GCS コネクタ \+ `aggregate.py`）を Push、`raw/` の小データを読んで `curated/` に Parquet 出力。  
  - DoD：GCS に正しい Parquet が出る／History Server でステージが見える。  
- **Phase 3｜オーケストレーション**：Airflow を Helm で導入、`SparkKubernetesOperator` で日次 DAG 化、リトライ・`{{ ds }}` 伝播・**冪等な再実行**を確認。  
  - DoD：同じ日付を 2 回流して結果が一意。バックフィルが動く。  
- **Phase 4｜スキュー再現と調整**：偏りデータでスキューを発生させ、AQE/salting で改善を計測。  
  - DoD：対策前後で wall-clock の改善を Spark UI で確認。  
- **Phase 5｜観測性とコスト**：History Server 常設、Cloud Logging、（任意）Prometheus/Grafana、Spot \+ min0 のスケール 0 を確認。  
  - DoD：アイドル時に spark ノードが 0 台になる。  
- **Phase 6｜（任意）HDFS 学習**：付録 G の最小 HDFS \+ MapReduce を一度動かし、HDFS/YARN の感触を得る。  
  - DoD：HDFS に put/get、MapReduce が 1 本通る。

---

## 14\. リスクと対策

| リスク | 影響 | 対策 |
| :---- | :---- | :---- |
| コスト超過 | 課金が膨らむ | Spot、autoscaling min 0、使わない時はクラスタ削除、予算アラート |
| shuffle ディスク不足 | ジョブ失敗 | ローカル SSD 追加、パーティション/メモリ調整、データ縮小 |
| 小ファイル乱立 | 読み取り劣化・メタ肥大 | 書き込み前 `repartition/coalesce`、ファイルサイズ目標 128〜256MB |
| バージョン非互換 | 起動しない | Spark 3.5系 \+ Java 17 \+ GCS コネクタ hadoop3 系で揃える（§15）。最新版は都度確認 |
| Operator の Service 蓄積（大量ジョブ時） | 詰まり | `timeToLiveSeconds` で完了オブジェクト GC |
| 認証エラー | GCS 403 | Workload Identity の GSA⇄KSA バインドとアノテーションを再確認 |

---

## 15\. 推奨バージョン（着手時に最新を確認すること）

互換性最優先のため、**Spark は 3.5 系 LTS**（拡張サポートが 2027/11 まで）を基準にする。Spark 4.0.x / 4.1.x は最新だが、Operator・GCS コネクタ・Airflow provider の対応状況を見てから採用判断する。

- Spark：**3.5 系の最新パッチ**（Scala 2.12 / Java 17）  
- GKE：Standard、Release channel \= regular、Workload Identity 有効  
- Kubeflow Spark Operator：Helm chart v2.x（repo `https://kubeflow.github.io/spark-operator`）  
- GCS コネクタ：`gcs-connector-hadoop3` の shaded jar（最新を確認）  
- Airflow：2.x または 3.x（Helm official chart）+ `apache-airflow-providers-cncf-kubernetes`

---

## 16\. 決定ログ / Open Questions

**決定**

- ストレージは GCS（HDFS を本筋にしない）。  
- 実行は Spark Operator（素 submit にしない）。  
- オーケストレーションは自前 Airflow（Composer にしない）。  
- 冪等性は「論理日付パーティション ＋ staging→publish ＋ `_SUCCESS` センチネル」を基本線（GCS では動的上書き単独は非原子なため本線にしない）。  
- バックフィルは Airflow `catchup=True` ＋ `max_active_runs=1` で自動実行。`run_date` は処理対象データの論理日付（実行日ではない）。

**Open Questions**

- 原子的コミットの本筋として **テーブルフォーマット（Apache Iceberg / Delta Lake）** を導入すべきか？ これらは GCS 上でも**メタデータのポインタ swap による原子的コミット**を提供し、パーティション上書き・スナップショットを正しく扱える（§8 の根本解）。今回は学習スコープのため staging→publish ＋ `_SUCCESS` で回し、Iceberg は「次の一手」として保留。  
- メタストア（Hive/BigLake）を入れるか？ 当面は非ゴール。  
- スキューの本格チューニング（salting の自動化等）はどこまで？ 第一歩は「検知」に置く。

---

## 付録 A — GKE / Workload Identity 構築コマンド

\# 変数（自分の値に置き換え）

export PROJECT\_ID=my-project

export REGION=asia-northeast1

export LAKE=gs://${PROJECT\_ID}-datalake

\# GCS バケット（3層はプレフィックスで分けるのでバケットは1つでOK）

gcloud storage buckets create ${LAKE} \--location=${REGION} \--uniform-bucket-level-access

\# GKE クラスタ（Workload Identity 有効）

gcloud container clusters create spark-batch \\

  \--region ${REGION} \--release-channel regular \\

  \--num-nodes 1 \--machine-type e2-standard-4 \\

  \--workload-pool=${PROJECT\_ID}.svc.id.goog \--enable-ip-alias

\# Spark Executor 用 Spot ノードプール（スケール0〜）

gcloud container node-pools create spark-spot \\

  \--cluster spark-batch \--region ${REGION} \\

  \--machine-type n2-standard-8 \--spot \\

  \--enable-autoscaling \--min-nodes 0 \--max-nodes 8 \\

  \--node-labels=workload=spark \\

  \--node-taints=workload=spark:NoSchedule

  \# 大きめ shuffle 用に: \--ephemeral-storage-local-ssd count=1

\# namespace

kubectl create ns spark-jobs

\# \--- Workload Identity: GSA を作って GCS 権限を付与し、KSA にバインド \---

gcloud iam service-accounts create spark-gsa \--display-name="Spark GCS access"

gcloud storage buckets add-iam-policy-binding ${LAKE} \\

  \--member="serviceAccount:spark-gsa@${PROJECT\_ID}.iam.gserviceaccount.com" \\

  \--role="roles/storage.objectAdmin"

\# KSA（Spark ジョブ用）を作成（Operator が作る SA を使う場合はそれに合わせる）

kubectl create serviceaccount spark \-n spark-jobs

\# KSA \-\> GSA バインド

gcloud iam service-accounts add-iam-policy-binding \\

  spark-gsa@${PROJECT\_ID}.iam.gserviceaccount.com \\

  \--role="roles/iam.workloadIdentityUser" \\

  \--member="serviceAccount:${PROJECT\_ID}.svc.id.goog\[spark-jobs/spark\]"

\# KSA にアノテーション

kubectl annotate serviceaccount spark \-n spark-jobs \\

  iam.gke.io/gcp-service-account=spark-gsa@${PROJECT\_ID}.iam.gserviceaccount.com

最小 RBAC（Driver が Executor Pod を作れるように）:

\# spark-rbac.yaml

apiVersion: rbac.authorization.k8s.io/v1

kind: Role

metadata: { name: spark-role, namespace: spark-jobs }

rules:

  \- apiGroups: \[""\]

    resources: \["pods", "services", "configmaps", "persistentvolumeclaims"\]

    verbs: \["create", "get", "list", "watch", "delete", "deletecollection"\]

\---

apiVersion: rbac.authorization.k8s.io/v1

kind: RoleBinding

metadata: { name: spark-rb, namespace: spark-jobs }

subjects: \[{ kind: ServiceAccount, name: spark, namespace: spark-jobs }\]

roleRef: { kind: Role, name: spark-role, apiGroup: rbac.authorization.k8s.io }

---

## 付録 B — Spark Operator / Airflow 導入（Helm）

\# Spark Operator

helm repo add spark-operator https://kubeflow.github.io/spark-operator

helm repo update

helm install spark-operator spark-operator/spark-operator \\

  \--namespace spark-operator \--create-namespace \\

  \--set 'spark.jobNamespaces={spark-jobs}' \\

  \--set webhook.enable=true      \# volume/configmap マウントに必要

\# Airflow（official chart）

helm repo add apache-airflow https://airflow.apache.org

helm repo update

kubectl create ns airflow

helm install airflow apache-airflow/airflow \--namespace airflow \\

  \--set executor=KubernetesExecutor

\# DAG は git-sync か、イメージ同梱で配布（chart の values で設定）

---

## 付録 C — Spark \+ GCS コネクタの自前イメージ

\# Dockerfile

FROM apache/spark:3.5.6      \# ← 3.5 系の最新パッチを確認して指定

USER root

\# GCS コネクタ（hadoop3 shaded）。URL/バージョンは最新を確認

ADD https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \\

    /opt/spark/jars/gcs-connector-hadoop3.jar

\# PySpark ジョブを焼き込む

COPY jobs/ /opt/spark/jobs/

USER spark

\# ビルドして Artifact Registry へ

export IMG=${REGION}-docker.pkg.dev/${PROJECT\_ID}/spark/spark-gcs:3.5.6

gcloud artifacts repositories create spark \--repository-format=docker \--location=${REGION} || true

docker build \-t ${IMG} .

docker push ${IMG}

---

## 付録 D — SparkApplication / ScheduledSparkApplication

`spark_application.yaml`（Airflow からテンプレート展開して apply）:

apiVersion: sparkoperator.k8s.io/v1beta2

kind: SparkApplication

metadata:

  name: kaggle-agg-{{ ds\_nodash }}

  namespace: spark-jobs

spec:

  type: Python

  mode: cluster

  image: REGION-docker.pkg.dev/PROJECT/spark/spark-gcs:3.5.6

  imagePullPolicy: IfNotPresent

  mainApplicationFile: local:///opt/spark/jobs/aggregate.py

  arguments: \["--run-date", "{{ ds }}"\]

  sparkVersion: "3.5.6"

  timeToLiveSeconds: 86400          \# 完了オブジェクトの自動GC

  restartPolicy:

    type: OnFailure

    onFailureRetries: 3

    onFailureRetryInterval: 30

  sparkConf:

    "spark.sql.adaptive.enabled": "true"

    "spark.sql.adaptive.skewJoin.enabled": "true"

    "spark.sql.adaptive.coalescePartitions.enabled": "true"

    "spark.sql.shuffle.partitions": "200"

    "spark.sql.sources.partitionOverwriteMode": "dynamic"   \# シンプル版/小データ用。GKE 本線は staging→publish（§8）

    "spark.eventLog.enabled": "true"

    "spark.eventLog.dir": "gs://PROJECT-datalake/spark-events/"

    \# GCS コネクタ \+ Workload Identity（ADC）

    "spark.hadoop.fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"

    "spark.hadoop.fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"

    "spark.hadoop.fs.gs.auth.type": "APPLICATION\_DEFAULT"    \# ← コネクタ版により名称差異あり。要確認

  driver:

    cores: 1

    memory: "2g"

    serviceAccount: spark

    \# Driver は安定した system プールに（Spot 回避）

  executor:

    cores: 2

    instances: 3

    memory: "4g"

    serviceAccount: spark

    \# Executor は Spot プールへ

    nodeSelector: { workload: spark }

    tolerations:

      \- key: workload

        operator: Equal

        value: spark

        effect: NoSchedule

日次を Operator 側で回すなら `ScheduledSparkApplication`（Airflow を使わない最小構成の選択肢）:

apiVersion: sparkoperator.k8s.io/v1beta2

kind: ScheduledSparkApplication

metadata: { name: kaggle-agg-daily, namespace: spark-jobs }

spec:

  schedule: "0 18 \* \* \*"          \# cron

  concurrencyPolicy: Forbid       \# 多重起動を抑止（冪等と併せて安全side）

  template:

    \# ↑ SparkApplication の spec をここに（引数は run-date を実行日に）

    type: Python

    mode: cluster

    image: ...

    mainApplicationFile: local:///opt/spark/jobs/aggregate.py

---

## 付録 E — PySpark 変換ジョブ（冪等・スキュー対応の雛形）

\# jobs/aggregate.py

import argparse

from pyspark.sql import SparkSession, functions as F

def main():

    ap \= argparse.ArgumentParser()

    ap.add\_argument("--run-date", required=True)

    args \= ap.parse\_args()

    spark \= SparkSession.builder.appName("kaggle-agg").getOrCreate()

    \# AQE 等は SparkApplication の sparkConf 側で指定済み

    LAKE \= "gs://PROJECT-datalake"

    \# 論理日付スライスだけ読む（対象データの日付。実行日ではない）

    raw\_path \= f"{LAKE}/raw/events/dt={args.run\_date}/"

    \# 本線：staging に書き、DQ 合格後に publish タスクが curated へ昇格（§7・§8）

    out\_path \= f"{LAKE}/staging/agg\_by\_category/"

    df \= (spark.read.option("header", True).option("inferSchema", True).csv(raw\_path))

    \# 重複に強くする（必要に応じてビジネスキーで）

    df \= df.dropDuplicates()

    agg \= (

        df.groupBy("category")

          .agg(F.count(F.lit(1)).alias("cnt"),

               F.sum(F.col("amount").cast("double")).alias("total"))

          .withColumn("run\_date", F.lit(args.run\_date))

    )

    \# 出力（カテゴリ別集計）は小さいので少数ファイルに集約。大きい場合のみ

    \# サイズ見積りから N を決め repartition(N)。run\_date は全行同一値のため

    \# repartition("run\_date") は単一パーティション化してしまうので使わない。

    agg \= agg.coalesce(1)

    \# 冪等：staging の run\_date パーティションを上書き（再実行安全）。

    \# curated への確定は publish タスク＋_SUCCESS センチネルで担保（§8）。

    (agg.write

        .mode("overwrite")

        .partitionBy("run\_date")

        .parquet(out\_path))

    spark.stop()

if \_\_name\_\_ \== "\_\_main\_\_":

    main()

salting を試す場合の例（偏りキー対策）：

SALT \= 16

salted \= df.withColumn("salt", (F.rand()\*SALT).cast("int"))

part \= salted.groupBy("category", "salt").agg(F.sum("amount").alias("p"))

final \= part.groupBy("category").agg(F.sum("p").alias("total"))

---

## 付録 F — Airflow DAG（SparkKubernetesOperator）

\# dags/kaggle\_batch.py

import datetime

from airflow import DAG

from airflow.providers.cncf.kubernetes.operators.spark\_kubernetes import SparkKubernetesOperator

\# provider 版により完了待ち/既存オブジェクトの扱いが異なる。完了待ちは SparkKubernetesSensor を併用。

\# 同名 SparkApplication の再実行衝突は「apply 前に delete（冪等 apply）」で回避（§6.4）。要 provider 版確認。

default\_args \= {"retries": 2, "retry\_delay": datetime.timedelta(minutes=5)}

with DAG(

    dag\_id="kaggle\_batch",

    start\_date=datetime.datetime(2026, 6, 1),

    schedule="@daily",

    catchup=True,                 \# バックフィルを自動実行（§3.1）

    max\_active\_runs=1,            \# 多重起動の抑止（concurrencyPolicy=Forbid 相当）

    default\_args=default\_args,

    template\_searchpath="/opt/airflow/dags",   \# spark\_application.yaml を置く場所

) as dag:

    spark\_aggregate \= SparkKubernetesOperator(

        task\_id="spark\_aggregate",

        namespace="spark-jobs",

        application\_file="spark\_application.yaml",  \# {{ ds }} 等を埋め込み

        kubernetes\_conn\_id="kubernetes\_default",

        do\_xcom\_push=True,

    )

    \# 例：DQ チェック（PythonOperator / KubernetesPodOperator などで実装）

    \# validate\_dq \= ...

    \# spark\_aggregate \>\> validate\_dq

---

## 付録 G —（任意）HDFS/YARN を一度だけ触る学習モジュール

本筋（GCS）には使わないが、「Hadoop を立てた」という体験のために、最小 HDFS を別 namespace に立てて MapReduce を 1 本通すだけのモジュール。**学習専用・常設しない**。

- 方針：`apache/hadoop` 系イメージで、NameNode 1 \+ DataNode 1〜2 の StatefulSet（PVC 付き）を立て、`hdfs dfs -put` でファイルを置き、サンプル MapReduce（wordcount）を YARN 上で実行。  
- 注意：DataNode は状態を持つため PVC が要る／構成が重い。終わったら削除してコストを止める。  
- 位置づけ：HDFS の put/get と YARN のジョブ実行の「感触」を得るのが目的。パイプライン本体は GCS を使う。

（最小構成の骨子）

StatefulSet: hdfs-namenode  (PVC: name dir)

StatefulSet: hdfs-datanode  (replicas: 1-2, PVC: data dir)

Service(headless): namenode RPC/HTTP, datanode

→ kubectl exec で: hdfs dfs \-mkdir/-put、yarn jar ... wordcount

---

## 付録 H — 参考リンク

- Apache Spark（Downloads / Docs）: [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)  
- Spark Running on Kubernetes: [https://spark.apache.org/docs/latest/running-on-kubernetes.html](https://spark.apache.org/docs/latest/running-on-kubernetes.html)  
- Kubeflow Spark Operator（Getting Started / User Guide / GCP guide）: [https://www.kubeflow.org/docs/components/spark-operator/](https://www.kubeflow.org/docs/components/spark-operator/)  
- Spark Operator GitHub: [https://github.com/kubeflow/spark-operator](https://github.com/kubeflow/spark-operator)  
- GKE Workload Identity: [https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)  
- GCS connector for Hadoop: [https://github.com/GoogleCloudDataproc/hadoop-connectors](https://github.com/GoogleCloudDataproc/hadoop-connectors)  
- Airflow Helm Chart: [https://airflow.apache.org/docs/helm-chart/stable/index.html](https://airflow.apache.org/docs/helm-chart/stable/index.html)  
- Airflow cncf.kubernetes provider（SparkKubernetesOperator）: [https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/](https://airflow.apache.org/docs/apache-airflow-providers-cncf-kubernetes/stable/)

注：バージョン・コマンド・設定キー（特に GCS コネクタの `fs.gs.auth.type` 周り、Airflow provider の完了待ち API）は更新が早い。**着手時に各公式ドキュメントで最新を確認**すること。  