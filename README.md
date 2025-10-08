
---

# データの品質管理と通知機能を持つサーバレスパイプライン

## 概要 (Project Summary)

本プロジェクトは、**YouTube Data API** から取得したデータを収集・変換し、**データの品質を保証**しながらデータレイク ($\text{S3}$) に格納する**サーバレス**な $\text{ETL}$ パイプラインです。

データ変換処理中に**データ品質チェック**や**パイプラインの実行チェック**を組み込み、異常を検知した際には即座に担当者へアラート通知を行うことで、**運用安定性とデータの信頼性**を両立させています。

---

## アーキテクチャ (Architecture)

全体のワークフローは $\text{AWS}$ $\text{Step}$ $\text{Functions}$ ($\text{SFN}$) によってオーケストレーションされており、$\text{CloudWatch}$ $\text{Alarms}$ により全ての異常が監視・通知されます。

構成図については、以下のコードを実行して生成される画像を参照してください。

**図のコードの場所:** `structure/archivement.py`



---

## 技術スタック (Tech Stack)

| カテゴリ | サービス / 技術 | 用途 |
| :--- | :--- | :--- |
| **オーケストレーション** | **AWS Step Functions** (`SukimaDataPipeline-ETL-Workflow`) | 全体の処理フローの制御、エラー処理、リトライロジック。 |
| **データ処理** | **AWS Glue (Spark)** (`glue-sukima-transform`) | 大規模なデータ変換 (ETL)、**データ品質 (DQ) チェック**実行。 |
| **データ収集** | **AWS Lambda** ($\text{Python}$) | $\text{YouTube}$ $\text{Data}$ $\text{API}$ からのデータスクレイピング。 |
| **データレイク** | **Amazon S3** | 生データおよび加工済みデータの格納（データレイク）。 |
| **データ分析** | **Amazon Athena** | $\text{S3}$ に格納されたデータへのクエリ実行・分析基盤。 |
| **スケジュール** | **Amazon EventBridge** | $\text{SFN}$ ワークフローの定期的な実行スケジュール管理。 |
| **監視・通知** | **Amazon CloudWatch, Amazon SNS** | パイプラインの状態、パフォーマンス、DQ異常の監視と通知。 |

---

## パイプライン機能詳細 (Pipeline Functionality)

### 1. データフローの概要

1.  **EventBridge** のスケジュールに基づき **Step Functions** が起動。
2.  **Lambda (Scraper)** が $\text{YouTube}$ $\text{API}$ からデータを取得し、$\text{S3}$ に生データを格納。
3.  **Glue Job** が $\text{S3}$ から生データを読み込み、データ型変換などの $\text{Transform}$ 処理を実行。
4.  **Glue Job** 内で**データ品質 (DQ) チェック**を実行。
5.  加工済みデータを $\text{S3}$ に書き戻し、$\text{Athena}$ のデータカタログを更新。

### 2. データ品質 (DQ) チェックの詳細

| データセット | チェック対象（プライマリキー） | DQルール (主要なもの) |
| :--- | :--- | :--- |
| **channel** | `channel_id` | **一意性 ($\text{IsUnique}$)**、**非 $\text{NULL}$ ($\text{IsComplete}$)** |
| **video** | `video_id` | **一意性 ($\text{IsUnique}$)**、**非 $\text{NULL}$ ($\text{IsComplete}$)** |
| **comment** | `comment_id` | **一意性 ($\text{IsUnique}$)**、**非 $\text{NULL}$ ($\text{IsComplete}$)** |

> **補足**: `published_at` や `total_seconds` の**データ網羅性 ($\text{Completeness}$) が $\text{90}\%$ 未満**の場合も $\text{DQ}$ 異常として検知されます。特に、`published_id` をキーとする重複値処理の品質を担保するための、型変換やデータ総量のチェックを内部で実施しています。

---

## 監視とアラート対応 (Monitoring & Alert Handling)

### 1. 監視アラームの種類

| 種類 | 監視対象 | 目的 |
| :--- | :--- | :--- |
| **失敗アラーム** | $\text{SFN}$ ($\text{ExecutionsFailed}$ メトリクス) | パイプライン全体の失敗を即時検知。 |
| **時間超過アラーム** | $\text{SFN}$ ($\text{Duration}$ メトリクス) | $\text{15}$ 分を超過した際のハングアップを検知。 |
| **DQ失敗アラーム** | $\text{Glue}$ $\text{Job}$ ($\text{DQ}$ $\text{Failed}$ $\text{Count}$ メトリクス) | データ品質の異常を検知。 |

### 2. 対応フロー

全てのアラームは $\text{SNS}$ $\text{Topic}$ を経由し、**メールアドレス**に通知されます。

1.  **通知受領**: メールでアラート通知を受け取ります。
2.  **アラーム種類確認**: アラーム名から、**パイプライン失敗**か**データ品質異常**かを判断します。
3.  **ログ確認**:
    * **SFN失敗の場合**: $\text{SFN}$ の実行履歴から、失敗した $\text{State}$ を特定し、リンク先の $\text{Glue}$ $\text{Job}$ または $\text{Lambda}$ の $\text{CloudWatch}$ $\text{Logs}$ を確認します。
    * **DQ失敗の場合**: $\text{Glue}$ $\text{Job}$ のログで、具体的に**どの $\text{DQ}$ $\text{Rule}$ が失敗したか**を確認します。
4.  **原因特定と対処**: エラーメッセージに基づき、ソースデータの問題か、コードのバグかを特定し、対処します。

---

## 運用と手動実行 (Operation & Manual Execution)

### 1. 自動実行スケジュール

本パイプラインは $\text{EventBridge}$ によって毎週金曜日に自動実行されます。

### 2. 手動での再実行手順

テストやリカバリのために手動で実行する場合、以下の $\text{AWS}$ コンソールから行います。

1.  **Step Functions コンソール**へアクセス。
2.  ステートマシン名: **`SukimaDataPipeline-ETL-Workflow`** を検索。
3.  「**実行の開始**」ボタンをクリックし、実行を開始します。

### 3. Glue Job 単体のデバッグ実行

データ変換部分を個別にデバッグする場合、以下の $\text{AWS}$ コンソールから行います。

1.  **Glue コンソール**へアクセス。
2.  ジョブ名: **`glue-sukima-transform`** を検索。
3.  「**アクション**」から「**ジョブの実行**」を選択します。