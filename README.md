概要

本プロジェクトは、JRA競馬のレースごとに馬番単位でオッズの変動率を取得・分析するシステムです。以下の機能を提供します。

レース開始1時間前、30分前、5分前のオッズをスクレイピングで取得して BigQuery に格納

各タイミング間の変動率を計算し、BigQuery に登録

Google スプレッドシートの BigQuery データコネクタを利用した可視化

開発環境

プラットフォーム: GitHub Codespaces

言語: Python 3.9+

ライブラリ:

requests, beautifulsoup4 (スクレイピング)

pandas (データ処理)

google-cloud-bigquery (BigQuery クライアント)

apscheduler (ジョブスケジューラ)

データベース: Google BigQuery（無料枠）

可視化: Google スプレッドシート（BigQuery データコネクタ）

ディレクトリ構成例

/ (リポジトリルート)
├── .devcontainer/          # Codespaces 設定
├── data/                   # 一時データ（必要に応じて）
├── scripts/                # ジョブ起動スクリプト
│   ├── schedule_jobs.py    # スケジューリング登録
│   ├── job_fetch_store.py  # オッズ取得＆BigQuery登録
│   ├── calc_fluctuation.py # 変動率計算バッチ
├── requirements.txt        # 依存ライブラリ
├── README.md               # 本ファイル
└── .env.example            # 環境変数サンプル

セットアップ手順

リポジトリをクローン

git clone git@github.com:<ユーザー>/<リポジトリ>.git
cd <リポジトリ>

Codespaces を起動
GitHub 上で「Code」→「Open with Codespaces」を選択します。

仮想環境構築 & ライブラリインストール

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

GCP 認証情報の設定

GCP コンソールでサービスアカウントキー(JSON)を作成

Codespaces の Secrets に GCP_CREDENTIALS_JSON として登録

以下を実行:

echo "$GCP_CREDENTIALS_JSON" > ~/workspace/creds.json
export GOOGLE_APPLICATION_CREDENTIALS=~/workspace/creds.json

BigQuery データセット & テーブル作成

scripts/setup_bigquery.py を実行し、データセット・テーブルを自動作成

ジョブスケジューラ登録

python scripts/schedule_jobs.py

Google スプレッドシート連携

新規スプレッドシートを作成

メニュー「データ」→「データコネクタ」→「BigQuery に接続」

プロジェクト→データセットjra_odds→テーブルodds_snapshot, odds_fluctuation を追加

実行方法

データ収集ジョブの起動

python scripts/schedule_jobs.py

変動率計算バッチ

手動実行:

python scripts/calc_fluctuation.py --race_id <race_id>

自動実行: APScheduler または Cloud Scheduler に登録可能

環境変数

名称

説明

GCP_CREDENTIALS_JSON

GCP サービスアカウントキー(JSON 全文)

GCP_PROJECT_ID

GCP プロジェクト ID

※その他、必要に応じて .env ファイルに追記してください。

テスト

ユニットテストは pytest で実行可能:

pytest

ライセンス

MIT License
