# scripts/fetch_odds.py

import os
import sys
import time
import random
import logging
import re
from datetime import datetime, timezone

from playwright.sync_api import sync_playwright
from google.cloud import bigquery

# --- 共通設定 ---
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) "
    "Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:115.0) "
    "Gecko/20100101 Firefox/115.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 OPR/85.0.4341.72",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 OPR/85.0.4341.72",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Vivaldi/5.3.2679.55",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Vivaldi/5.3.2679.55",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Brave/1.40.107",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Brave/1.40.107",
]

BQ_PROJECT = bigquery.Client().project
BQ_DATASET = "jra_odds"
BQ_TABLE   = f"{BQ_PROJECT}.{BQ_DATASET}.odds"

# --- 単勝オッズ取得 ---
def fetch_odds_by_race_id(race_id: str, minutes_before_race: int) -> list[dict]:
    odds_url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    print(f"[INFO] オッズページロード → {odds_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1200, "height": 800}
        )
        try:
            page.goto(odds_url, timeout=0)
            page.wait_for_selector("table.Shutuba_Table", timeout=10000)

            odds_list = []
            rows = page.query_selector_all("table.Shutuba_Table tbody tr.HorseList")

            for row in rows:
                try:
                    number_el = row.query_selector("td[class^='Umaban']")
                    number_txt = number_el.inner_text().strip() if number_el else ""
                    number = int(number_txt) if number_txt.isdigit() else None

                    # 特定クラス名に依存せず、<td class="Txt_R Popular"> 内の <span> を取得
                    odds_td = row.query_selector("td.Txt_R.Popular")
                    odds_span = odds_td.query_selector("span") if odds_td else None
                    odds_txt = odds_span.inner_text().strip() if odds_span else ""
                    odds = float(odds_txt) if re.match(r"^\d+(\.\d+)?$", odds_txt) else None

                    if number is not None and odds is not None:
                        odds_list.append({
                            "number": number,
                            "odds": odds,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        })
                except Exception as e:
                    logging.warning(f"[WARNING] 行パース失敗: {e}")

            return {
                "race_id": race_id,
                "minutes_before_race": minutes_before_race,
                "odds_list": odds_list
            }

        finally:
            browser.close()


# --- BigQuery 登録処理 ---
def store_odds_to_bigquery(odds_data: list[dict]):
    client = bigquery.Client()

    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{BQ_TABLE}` (
        race_id   STRING,
        minutes_before_race INT64,
        odds_list ARRAY<STRUCT<
            number INT64,
            odds   FLOAT64,
            timestamp TIMESTAMP
        >>
    )
    """
    client.query(ddl).result()
    time.sleep(1)

    errors = client.insert_rows_json(BQ_TABLE, odds_data)
    if errors:
        print(f"[ERROR] BigQuery登録失敗: {errors}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] {len(odds_data)} 件のオッズを登録しました → {BQ_TABLE}")


# --- エントリーポイント ---
def fetch_odds():
    if len(sys.argv) < 3:
        print("Usage: python fetch_odds.py <race_id> <minutes_before_race>", file=sys.stderr)
        sys.exit(1)

    race_id = sys.argv[1]
    try:
        minutes_before_race = int(sys.argv[2])
    except ValueError:
        print("minutes_before_race must be an integer.", file=sys.stderr)
        sys.exit(1)
        
    odds_data = fetch_odds_by_race_id(race_id, minutes_before_race)

    if not odds_data:
        print(f"[WARN] オッズ取得失敗: race_id={race_id}", file=sys.stderr)
        sys.exit(1)

    store_odds_to_bigquery([odds_data]) 


if __name__ == "__main__":
    fetch_odds()
