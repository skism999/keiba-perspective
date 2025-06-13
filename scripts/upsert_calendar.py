# scripts/upsert_calendar.py
import os
from datetime import datetime, date
from icalendar import Calendar
from google.cloud import bigquery

# 環境変数 or デフォルトパス
ICS_PATH = os.getenv('JRA_CALENDAR_ICS', 'data/race_calendar/jrarace2025.ics')
PROJECT = bigquery.Client().project
DATASET = 'jra_odds'

def fetch_calendar_from_ics(path: str) -> list[dict]:
    """ICS ファイルから race_date(date) と track(string) を抽出"""
    with open(path, 'rb') as f:
        cal = Calendar.from_ical(f.read())
    events = []
    for comp in cal.walk():
        if comp.name != 'VEVENT':
            continue
        dt = comp.get('DTSTART').dt
        # date 型に統一
        rd = dt if isinstance(dt, date) else dt.date()
        track = str(comp.get('LOCATION')).strip()
        events.append({'race_date': rd, 'track': track})
    return events

def upsert_calendar():
    client = bigquery.Client()
    rows = fetch_calendar_from_ics(ICS_PATH)
    if not rows:
        print("No calendar entries found.")
        return

    # 日付順にソート
    rows.sort(key=lambda r: r['race_date'])

    # テーブル名を "{year}_race_calendar" とする
    year = rows[0]['race_date'].year
    table_id = f"{PROJECT}.{DATASET}.{year}_race_calendar"

    # DDL：テーブルがなければ作成
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{table_id}` (
      race_date DATE,
      track     STRING
    )
    """
    client.query(ddl).result()
    print(f"Ensured table exists: {table_id}")

    # BigQuery に挿入
    # JSON にする際、日付は ISO フォーマット文字列で渡す
    bq_rows = [
        {'race_date': r['race_date'].isoformat(), 'track': r['track']}
        for r in rows
    ]
    errors = client.insert_rows_json(table_id, bq_rows)
    if errors:
        print("Insert errors:", errors)
    else:
        print(f"Inserted {len(bq_rows)} rows into {table_id}.")

if __name__ == '__main__':
    upsert_calendar()
