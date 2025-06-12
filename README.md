# JRA競馬オッズ変動率モニタリングシステム

## 📖 プロジェクト概要
JRA公式サイトと netkeiba.com の両方からデータを取得し、各レース・各馬ごとのオッズ情報を取得・集計・可視化するシステムです。以下を実現します。

- **レーシングカレンダー取得**：当年分の開催日と開催競馬場を JRA「レーシングカレンダー」から取得  
- **レース情報取得**：開催日の日本時間9:00に、各競馬場12レース分のレース番号、発走時刻、レース名、レースクラス、芝/ダート、距離、頭数を取得  
- **出走馬情報取得**：各レースごとに枠順、馬番、馬体重、馬名、騎手を取得  
- **オッズ取得**：レース発走1時間前、30分前、5分前、レース後にJRA公式とnetkeibaの両方でスクレイピングし、平均値を算出  
- **変動率計算**：各馬ごとにオッズの変動値・変動率を算出  
- **結果登録**：Race結果（着順）を取得し、馬ごとに記録  
- **出力**：日ごとにスプレッドシートを作成し、競馬場・レースごとにシート分けしてデータを整理・共有  

取得・登録する項目例（シート列順）  
> 競馬場名 | レース番号 | 発走時刻 | レース名 | レースクラス | 芝/ダート | 距離 | 頭数 | 枠 | 馬番 | 馬体重 | 馬名 | 騎手 | レース1時間前オッズ | レース30分前オッズ | レース5分前オッズ | レース後オッズ | オッズ変動値 | オッズ変動率 | レース結果

## ⭐️ 主な特徴
- **データ冗長性**：JRA公式・netkeiba両ソースから取得し平均化  
- **自動スケジューリング**：開催日の朝9:00にカレンダー～レース情報を一括取得しバッチ登録  
- **多段階オッズ取得**：1h/30m/5m前とレース後の4タイミングで取得  
- **スプレッドシート整理**：日付単位のスプレッドシート、競馬場・レース単位のタブを自動作成  
- **BigQuery連携**：データウェアハウスに保存し、SQLで参照・集計可能  

## 🛠️ 開発環境
- **IDE／実行環境**：GitHub Codespaces  
- **言語**：Python 3.9+  
- **主要ライブラリ**:
  - `requests`, `beautifulsoup4`（スクレイピング）  
  - `pandas`（集計・変動率計算）  
  - `google-cloud-bigquery`（BigQuery クライアント）  
  - `apscheduler`（スケジューリング）  
  - `gspread`（スプレッドシート操作）  
- **データベース**：Google BigQuery（Sandbox/無料枠）  

## 📁 ディレクトリ構成（例）
```  
├── scripts/  
│   ├── upsert_calendar.py      # レーシングカレンダー取得・登録  
│   ├── upsert_races.py         # レース情報（12レース）取得・登録  
│   ├── upsert_entries.py       # 出走馬情報取得・登録  
│   ├── fetch_odds.py           # オッズ取得（JRA・netkeiba両対応）  
│   ├── scheduler.py            # APScheduler設定・ジョブ登録  
│   ├── calc_fluctuation.py     # 変動率計算・登録  
│   └── export_sheets.py        # 日別スプレッドシート生成・データ書き込み  
├── notebooks/                  # 動作確認用ノートブック  
├── requirements.txt            # Python依存パッケージ  
└── README.md                   # 本ドキュメント  
```  

## 🚀 セットアップ手順
1. **Codespaces起動**  
   - GitHubリポジトリを作成し「Open with Codespaces」をクリック  

2. **仮想環境作成 & ライブラリインストール**  
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```  

3. **GCP認証情報設定**  
   ```bash
   # Secretsに設定したJSONをファイル化
   echo "$GCP_CREDENTIALS_JSON" > ~/creds.json
   export GOOGLE_APPLICATION_CREDENTIALS=~/creds.json
   ```  

4. **BigQueryデータセット・テーブル作成**  
```sql
-- データセット作成（locationは必要に応じて指定）
CREATE SCHEMA IF NOT EXISTS `jra_odds` OPTIONS(location="asia-northeast1");

-- レース基本情報テーブル
CREATE TABLE IF NOT EXISTS `jra_odds.race` (
  race_id         STRING,
  race_date       DATE,
  start_time      DATETIME,
  race_name       STRING,
  race_class      STRING,
  track_surface   STRING,       -- 芝/ダート
  distance_m      INT64,        -- 距離（メートル）
  entries_count   INT64         -- 頭数
);

-- 出走馬情報テーブル
CREATE TABLE IF NOT EXISTS `jra_odds.entries` (
  race_id     STRING,
  frame_no    INT64,
  horse_no    INT64,
  weight_kg   INT64,
  horse_name  STRING,
  jockey      STRING
);

-- オッズスナップショットテーブル
CREATE TABLE IF NOT EXISTS `jra_odds.odds_snapshot` (
  race_id        STRING,
  horse_no       INT64,
  snapshot_at    DATETIME,
  odds_jra       FLOAT64,
  odds_netkeiba  FLOAT64,
  odds_avg       FLOAT64,
  label          STRING        -- '1h_before','30m_before','5m_before','post_race'
);

-- 変動率テーブル
CREATE TABLE IF NOT EXISTS `jra_odds.odds_fluctuation` (
  race_id           STRING,
  horse_no          INT64,
  from_label        STRING,
  to_label          STRING,
  fluctuation_value FLOAT64,
  fluctuation_rate  FLOAT64
);

-- レース結果テーブル
CREATE TABLE IF NOT EXISTS `jra_odds.race_results` (
  race_id         STRING,
  horse_no        INT64,
  finish_position INT64
);
```  


## 🏗️ 実装・運用手順
1. **レーシングカレンダー登録**  
   ```bash
   python scripts/upsert_calendar.py
   ```  

2. **レース＆出走馬情報登録**  
   ```bash
   python scripts/upsert_races.py
   python scripts/upsert_entries.py
   ```  

3. **オッズ取得スケジュール**  
   ```bash
   python scripts/scheduler.py
   ```  
   - 1h/30m/5m前とレース後の取得ジョブを登録  

4. **変動率計算**  
   ```bash
   python scripts/calc_fluctuation.py --race_id <race_id>
   ```  

5. **スプレッドシート生成・出力**  
   ```bash
   python scripts/export_sheets.py --date YYYY-MM-DD
   ```  

## 🤝 コラボレーション
- コラボレーター招待による共同開発が可能  
- PrivateリポジトリでもCodespaces/Actions無料枠内で利用可  

## 📜 ライセンス
MIT License
```
