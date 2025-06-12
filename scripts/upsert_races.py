import os
from datetime import datetime
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery

def fetch_races_for_date_and_track(date: datetime.date, track: str) -> list[dict]:
    # netkeiba 例: /race/calendar.html?year=2025&month=6&day=9&jyo=04
    # JRA 公式も同様にパラメタ調整
    url = f"https://race.netkeiba.com/calendar/?year={date.year}&month={date.month}&day={date.day}&jyo={track}"
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, "html.parser")
    races = []
    for tr in soup.select("table.RaceList tr")[1:13]:  # 12レース想定
        tds = tr.find_all("td")
        races.append({
            "race_id":    f"{date.strftime('%Y%m%d')}-{track}-{tds[0].text}",
            "race_date":  date,
            "start_time": datetime.combine(date, datetime.strptime(tds[2].text, "%H:%M").time()),
            "race_name":  tds[3].text.strip(),
            "race_class": tds[4].text.strip(),
            "track_surface": tds[5].text.strip(),
            "distance_m": int(tds[6].text.replace("m","")),
            "entries_count": int(tds[7].text),
        })
    return races

def upsert_races():
    client = bigquery.Client()
    # 今日の日付・track リストは calendar テーブルから取得
    query = "SELECT DISTINCT race_date, track FROM `jra_odds.calendar` WHERE race_date = CURRENT_DATE()"
    for row in client.query(query):
        date = row["race_date"]
        track = row["track"]
        races = fetch_races_for_date_and_track(date, track)
        errors = client.insert_rows_json(f"{client.project}.jra_odds.race", races)
        if errors:
            print(f"Errors inserting races for {track}:", errors)
        else:
            print(f"Inserted {len(races)} races for {track}.")

if __name__ == "__main__":
    upsert_races()
