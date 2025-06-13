import os
import sys
import time
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials
from google.cloud import bigquery

# --- 設定 ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
# サービスアカウント JSON は環境変数 GOOGLE_APPLICATION_CREDENTIALS で指す
CREDS = Credentials.from_service_account_file(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=SCOPES)
GC = gspread.authorize(CREDS)

BQ = bigquery.Client()
BQ_PROJECT = BQ.project
BQ_DATASET = "jra_odds"
BQ_TABLE_RACE = f"{BQ_PROJECT}.{BQ_DATASET}.race"

def export_schedule(target_date: str):
    """
    target_date: 'YYYY-MM-DD' 形式
    """
    # --- BigQuery から当日分のレース＋出馬表を展開して取得 ---
    ymd = target_date.replace("-", "")
    sql = f"""
      SELECT
        race_id, venue, race_no, race_name, start_time, track_surface,
        distance_m, race_class, entries_count, detail_url,
        horse.waku, horse.number, horse.name AS horse_name,
        horse.sex_age, horse.weight, horse.jockey, horse.trainer
      FROM `{BQ_TABLE_RACE}`, UNNEST(horses) AS horse
      WHERE SUBSTR(race_id, 1, 8) = '{ymd}'
      ORDER BY venue, race_no, horse.number
    """
    df = BQ.query(sql).result().to_dataframe()

    # --- 新規スプレッドシート作成 ---
    title = f"keiba odds {target_date}"
    sh = GC.create(title)
    print(f"[INFO] Created spreadsheet: {title}")

    # --- 会場＋レースNo ごとにシートを分けて出力 ---
    for (venue, race_no), group in df.groupby(["venue", "race_no"]):
        sheet_name = f"{venue}{race_no}R"
        # デフォルトに必ずある「Sheet1」は消して新規追加
        try:
            sh.del_worksheet(sh.worksheet("Sheet1"))
        except Exception:
            pass
        ws = sh.add_worksheet(title=sheet_name, rows=str(len(group) + 1), cols="20")

        # ヘッダー
        headers = [
            "race_id","venue","race_no","race_name","start_time",
            "track_surface","distance_m","race_class","entries_count",
            "detail_url","waku","number","horse_name","sex_age",
            "weight","jockey","trainer"
        ]
        ws.append_row(headers)

        # 各馬ごとに１行ずつ
        for _, row in group.iterrows():
            ws.append_row([
                row.race_id, row.venue, row.race_no, row.race_name,
                row.start_time, row.track_surface, row.distance_m,
                row.race_class, row.entries_count, row.detail_url,
                row.waku, row.number, row.horse_name, row.sex_age,
                row.weight, row.jockey, row.trainer
            ])
        print(f"[INFO] Wrote sheet {sheet_name} ({len(group)} rows)")

def export_race_schedule():
    if len(sys.argv) != 2:
        print("Usage: python export_race_schedule.py YYYY-MM-DD")
        sys.exit(1)
    target_date = sys.argv[1]
    # 前日１８時にスケジューラーで呼ぶ想定
    export_schedule(target_date)

if __name__ == "__main__":
    export_race_schedule()
