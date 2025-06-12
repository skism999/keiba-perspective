import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from datetime import datetime

def fetch_entries(race_id: str) -> list[dict]:
    # レース詳細ページ URL は環境に合わせて修正
    url = f"https://race.netkeiba.com/race/detail.html?race_id={race_id}"
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, "html.parser")
    entries = []
    for row in soup.select("table.EntryTable tr")[1:]:
        cols = row.find_all("td")
        entries.append({
            "race_id":   race_id,
            "frame_no":  int(cols[0].text),
            "horse_no":  int(cols[1].text),
            "weight_kg": int(cols[4].text),
            "horse_name": cols[2].text.strip(),
            "jockey":    cols[5].text.strip(),
        })
    return entries

def upsert_entries():
    client = bigquery.Client()
    query = "SELECT race_id FROM `jra_odds.race` WHERE race_date = CURRENT_DATE()"
    for row in client.query(query):
        rid = row["race_id"]
        entries = fetch_entries(rid)
        errors = client.insert_rows_json(f"{client.project}.jra_odds.entries", entries)
        if errors:
            print(f"Error inserting entries for {rid}:", errors)
        else:
            print(f"Inserted {len(entries)} entries for {rid}.")

if __name__ == "__main__":
    upsert_entries()
