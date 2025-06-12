# scripts/upsert_calendar.py
import os
from datetime import datetime
from icalendar import Calendar
from google.cloud import bigquery

# ICS ファイルパス（リポジトリ直下を想定）
ICS_PATH = 'data/race_calendar/jrarace2025.ics'
# BigQuery のカレンダーテーブル
BQ_TABLE = f"{bigquery.Client().project}.jra_odds.calendar"

def fetch_calendar_from_ics(path: str) -> list[dict]:
    """
    ICS の VEVENT を読み込み、race_date（DATE型文字列）と track（LOCATION）を返す。
    SUMMARY はレース名として出力用に使いたい場合に取得可能。
    """
    with open(path, 'rb') as f:
        cal = Calendar.from_ical(f.read())

    rows = []
    for comp in cal.walk():
        if comp.name != 'VEVENT':
            continue
        # DTSTART;VALUE=DATE:YYYYMMDD -> date オブジェクト
        dt = comp.get('DTSTART').dt
        # LOCATION フィールドに競馬場名
        track = str(comp.get('LOCATION')).strip()
        # SUMMARY にレース名（必要なら使える）
        summary = str(comp.get('SUMMARY')).strip()

        # race_date を ISO 形式文字列で
        date_str = dt.isoformat()  # '2025-08-31' など
        rows.append({
            'race_date': date_str,
            'track':     track,
            # 'summary': summary,  # 必要ならテーブルにカラム追加して使ってください
        })
    return rows

def upsert_calendar():
    client = bigquery.Client()
    entries = fetch_calendar_from_ics(ICS_PATH)
    if not entries:
        print("ICS に有効なイベントが見つかりませんでした。")
        return

    # BigQuery に一括挿入
    errors = client.insert_rows_json(BQ_TABLE, entries)
    if errors:
        print("BigQuery への挿入中にエラー発生:", errors)
    else:
        print(f"ICS から {len(entries)} 件のカレンダー情報を登録しました。")

if __name__ == '__main__':
    upsert_calendar()
