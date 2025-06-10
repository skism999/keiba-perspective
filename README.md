# JRA競馬オッズ変動率モニタリングシステム

📖 プロジェクト概要

JRA競馬の各レースにおいて、レース開始1時間前・30分前・5分前のオッズ情報を取得し、各馬ごとのオッズ変動率を算出・可視化するシステムです。取得データは Google BigQuery に格納し、Google スプレッドシートの「BigQuery データコネクタ」を介してリアルタイムに参照・分析できます。

⭐️ 主な特徴

- 定点オッズ取得：レース開始前1時間・30分・5分のタイミングでスクレイピング  
- 変動率計算：各馬ごとにオッズの変化率を自動算出  
- データ管理：BigQuery にテーブル保存、堅牢なデータウェアハウス  
- 可視化／共有：Google スプレッドシート連携によるリアルタイム更新  
- 柔軟なローカル開発：GitHub Codespaces＋Python で手軽にプロトタイプ  

🛠️ 開発環境

- **IDE／実行環境**：GitHub Codespaces  
- **言語**：Python 3.9+  
- **主要ライブラリ**:  
  - `requests`, `beautifulsoup4`（オッズスクレイピング）  
  - `pandas`（集計・変動率計算）  
  - `google-cloud-bigquery`（BigQuery クライアント）  
  - `apscheduler`（スケジューリング）  
- **データベース**：Google BigQuery（Sandbox/無料枠）  
- **可視化**：Google スプレッドシート（BigQuery データコネクタ）  

📁 ディレクトリ構成（例）


├── scripts/
│   ├── upsert_races.py       # レース情報取得・BigQuery登録
│   ├── fetch_odds.py         # オッズスクレイピング関数
│   ├── scheduler.py          # APScheduler 設定・ジョブ実行
│   └── calc_fluctuation.py   # 変動率算出バッチ
├── notebooks/                # 動作確認用ノートブック
├── requirements.txt         # Python 依存パッケージ
└── README.md                 # 本ドキュメント

🚀 セットアップ手順

1. **Codespaces 起動**  
   リポジトリを GitHub で公開 or private とし、「Open with Codespaces」をクリック。

2. **仮想環境作成**  

## GCP 認証情報設定
```bash
# GCP プロジェクトでサービスアカウントを作成し JSON キーをダウンロード
# Codespaces の Secrets に GCP_CREDENTIALS_JSON として設定
echo "$GCP_CREDENTIALS_JSON" > ~/creds.json
export GOOGLE_APPLICATION_CREDENTIALS=~/creds.json
```

## BigQuery データセット・テーブル作成
```sql
-- データセット作成
CREATE SCHEMA IF NOT EXISTS `jra_odds`;

-- レース基本情報テーブル
CREATE TABLE IF NOT EXISTS `jra_odds.race` (
  race_id     STRING PRIMARY KEY,
  race_date   DATE,
  start_time  DATETIME,
  race_class  STRING,
  result      STRING
);

-- オッズスナップショットテーブル
CREATE TABLE IF NOT EXISTS `jra_odds.odds_snapshot` (
  race_id      STRING,
  horse_no     INT64,
  snapshot_at  DATETIME,
  odds         FLOAT64,
  label        STRING
);

-- 変動率テーブル
CREATE TABLE IF NOT EXISTS `jra_odds.odds_fluctuation` (
  race_id      STRING,
  horse_no     INT64,
  from_label   STRING,
  to_label     STRING,
  rate_change  FLOAT64
);
```

## 🏗️ 実装・運用手順
```bash
# 1. レース情報の登録
python scripts/upsert_races.py

# 2. オッズ取得ジョブのスケジュール
python scripts/scheduler.py

# 3. 変動率バッチ実行 (レースID指定)
python scripts/calc_fluctuation.py --race_id <race_id>
```

## 📊 Google スプレッドシート連携
1. Google スプレッドシートを作成  
2. メニュー「データ」→「データコネクタ」→「BigQuery に接続」  
3. プロジェクト `jra_odds` → テーブル `odds_snapshot` / `odds_fluctuation` を選択  
4. シート上でフィルター・ピボットを設定し、動的に可視化  

## 🤝 コラボレーション
- コラボレーターを招待して共同開発が可能。  
- private リポジトリでも Codespaces / Actions は無料枠で利用可。  

## 📜 ライセンス
MIT License
