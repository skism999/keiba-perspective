# scripts/upsert_races.py

import os
import sys
import logging
import re
import random
import time
import requests
from bs4 import BeautifulSoup 
from datetime import datetime, date
from google.cloud import bigquery

from playwright.sync_api import sync_playwright

# User-Agent リスト（ランダムに選択）
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

# 競馬場名→コードマップ
TRACK_CODE_MAP = {
    "札幌": "01", "函館": "02", "福島": "03", "新潟": "04", "東京": "05",
    "中山": "06", "中京": "07", "京都": "08", "阪神": "09", "小倉": "10",
}

# --- 設定変数 ---
TARGET_DATE    = os.getenv("TARGET_DATE", date.today().isoformat())
KAISAI_DATE    = TARGET_DATE.replace("-", "")

# --- BigQuery client ---
BQ_PROJECT = bigquery.Client().project
BQ_DATASET = "jra_odds"
BQ_TABLE   = f"{BQ_PROJECT}.{BQ_DATASET}.race"


def build_race_urls(kaisai_date: str) -> list[dict]:
    """
    当日のレース一覧ページを開き、
    会場ごとに12レース分の出馬表 URL と venue 名を組み立てて返す。
    """
    list_url = f"https://race.netkeiba.com/top/race_list.html?kaisai_date={kaisai_date}"
    print(f"[INFO] レース一覧ページ: {list_url}")

    specs = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1200, "height": 800}
        )
        page.goto(list_url, timeout=0)
        page.wait_for_selector(".RaceList_DataTitle", timeout=20000)

        year = kaisai_date[:4]  # race_id は西暦４桁から

        for block in page.query_selector_all(".RaceList_DataList"):
            title = block.query_selector(".RaceList_DataTitle") \
                         .inner_text().strip()
            parts    = title.split()                # 例: ["3回","東京","4日目"]
            kaishu   = int(parts[0].replace("回",""))
            venue_nm = parts[1]                     # 会場名文字列
            nichime  = int(parts[2].replace("日目",""))

            code = TRACK_CODE_MAP.get(venue_nm)
            if code is None:
                print(f"[WARN] 未定義の会場名: {venue_nm}")
                continue

            for no in range(1,13):
                race_no = f"{no:02d}"
                race_id = (
                    f"{year}"
                    f"{code}"
                    f"{kaishu:02d}"
                    f"{nichime:02d}"
                    f"{race_no}"
                )
                url = (
                    "https://race.netkeiba.com/race/shutuba.html"
                    f"?race_id={race_id}"
                )
                specs.append({"url": url, "venue": venue_nm})
                print(f"[DEBUG] {venue_nm} → {url}")

        browser.close()

    time.sleep(random.uniform(1,2))
    return specs


def fetch_race_detail(page, url: str) -> dict:
    """
    出馬表ページを読み込み、
    レースNo・レース名・発走時刻・芝orダート・距離・クラス・頭数 を抽出する。
    """
    print(f"[INFO] 詳細ページロード → {url}")
    page.goto(url, timeout=0)
    page.wait_for_selector(".RaceNum", timeout=15000)

    # レースNo
    race_no_text = page.query_selector(".RaceNum").inner_text().strip()
    m = re.search(r"(\d+)R", race_no_text)
    race_no = int(m.group(1)) if m else 0

    # レース名
    race_name = page.query_selector("h1.RaceName").inner_text().strip()

    # RaceData01 から発走時刻／距離
    data01 = page.query_selector(".RaceData01").inner_text()
    tm = re.search(r"(\d{1,2}:\d{2})発走", data01)
    start_time = tm.group(1) if tm else ""
    dm = re.search(r"([芝ダ]\d+m)", data01)
    distance_text = dm.group(1) if dm else ""
    surface = "芝" if distance_text.startswith("芝") else "ダ"
    distance = int(re.sub(r"\D","", distance_text)) if distance_text else 0

    # RaceData02 内の <span> からクラス・頭数
    spans = page.query_selector(".RaceData02").query_selector_all("span")
    class_text = spans[4].inner_text().strip() if len(spans) > 4 else ""
    entries = 0
    for sp in spans:
        t = sp.inner_text().strip()
        if re.match(r"\d+頭", t):
            entries = int(re.sub(r"\D","",t))
            break

    detail = {
        "race_id":        url.split("race_id=")[-1],
        "race_no":        race_no,
        "race_name":      race_name,
        "start_time":     start_time,
        "track_surface":  surface,
        "distance_m":     distance,
        "race_class":     class_text,
        "entries_count":  entries,
        "detail_url":     url,
    }
    # horse データを取って付与
    detail["horses"] = fetch_horses(page)
    return detail

def fetch_horses(page) -> list[dict]:
    """
    現在開いている出馬表ページから
    ・枠 waku
    ・馬番 number
    ・馬名 name
    ・性齢 sex_age
    ・斤量 weight
    ・騎手 jockey
    ・厩舎 trainer
    ・予想オッズ
    を全行取得して返す。
    """
    page.wait_for_selector("table.Shutuba_Table tbody tr.HorseList", timeout=10000)
    rows = page.query_selector_all("table.Shutuba_Table tbody tr.HorseList")
    horses = []
    import re
    for r in rows:
        # 馬名の要素を先に確認し、無ければその行はスキップ
        name_el = r.query_selector("td.HorseInfo .HorseName a")
        if not name_el:
            logging.info("[INFO] データなし行をスキップ")
            continue

        # 枠
        try:
            waku_el = r.query_selector("td[class^='Waku'] span")
            waku_txt = waku_el.inner_text().strip() if waku_el else ""
        except Exception as e:
            logging.warning(f"[WARNING] 枠の取得失敗: {e}")
            waku_txt = ""

        # 馬番
        try:
            num_el = r.query_selector("td[class^='Umaban']")
            num_txt = num_el.inner_text().strip() if num_el else ""
        except Exception as e:
            logging.warning(f"[WARNING] 馬番の取得失敗: {e}")
            num_txt = ""

        # 馬名
        try:
            name = r.query_selector("td.HorseInfo .HorseName a").inner_text().strip()
        except Exception as e:
            logging.warning(f"[WARNING] 馬名の取得失敗: {e}")
            name = ""

        # 性別・年齢
        try:
            sex_age = r.query_selector("td.Barei").inner_text().strip()
        except Exception as e:
            logging.warning(f"[WARNING] 性齢の取得失敗: {e}")
            sex_age = ""

        # 馬体重
        try:
            wt_el = r.query_selector("td.Weight")
            wt_txt = wt_el.inner_text().strip() if wt_el else ""
            m_wt = re.search(r"(\d+)", wt_txt)
            weight_val = int(m_wt.group(1)) if m_wt else None
        except Exception as e:
            logging.warning(f"[WARNING] 馬体重の取得失敗: {e}")
            weight_val = None

        # 騎手
        try:
            jockey = r.query_selector("td.Jockey a").inner_text().strip()
        except Exception as e:
            logging.warning(f"[WARNING] 騎手の取得失敗: {e}")
            jockey = ""

        # 調教師
        try:
            trainer = r.query_selector("td.Trainer a").inner_text().strip()
        except Exception as e:
            logging.warning(f"[WARNING] 調教師の取得失敗: {e}")
            trainer = ""
        
        #予想オッズ
        try:
            odds_td = r.query_selector("td.Txt_R.Popular")
            odds_span = odds_td.query_selector("span") if odds_td else None
            odds_txt = odds_span.inner_text().strip() if odds_span else ""
            expect_odds = float(odds_txt) if re.match(r"^\d+(\.\d+)?$", odds_txt) else None
        except Exception as e:
            logging.warning(f"[WARNING] 予想オッズの取得失敗: {e}")
            odds = ""

        horses.append({
            "waku":     int(waku_txt) if waku_txt.isdigit() else None,
            "number":   int(num_txt)   if num_txt.isdigit()   else None,
            "name":     name,
            "sex_age":  sex_age,
            "weight":   weight_val,
            "jockey":   jockey,
            "trainer":  trainer,
            "expect_odds": expect_odds,
        })
    return horses

def upsert_entries():
    client = bigquery.Client()

    # ── テーブル作成 DDL ─────────────────────────────────────────
    # horses を REPEATED RECORD で定義
    ddl = f"""
    CREATE TABLE IF NOT EXISTS `{BQ_TABLE}` (
      race_id        STRING,
      venue          STRING,
      race_no        INT64,
      race_name      STRING,
      start_time     TIMESTAMP,
      track_surface  STRING,
      distance_m     INT64,
      race_class     STRING,
      entries_count  INT64,
      detail_url     STRING,
      horses         ARRAY<STRUCT<
        waku       INT64,
        number     INT64,
        name       STRING,
        sex_age    STRING,
        weight     FLOAT64,
        jockey     STRING,
        trainer    STRING,
        expect_odds FLOAT64
      >>
    )
    """
    time.sleep(3)
    client.query(ddl).result()
    time.sleep(3)
    print(f"[INFO] Ensured table exists: {BQ_TABLE}")
    # ─────────────────────────────────────────────────────────────

    specs = build_race_urls(KAISAI_DATE)
    races = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width":1200,"height":800}
        )

        for spec in specs:
            url   = spec["url"]
            venue = spec["venue"]         # → "東京" 等の文字列
            try:
                detail = fetch_race_detail(page, url)
                # 取得レコードに venue をセット
                detail["venue"] = venue

                # ISO フォーマット Timestamp に変換
                if detail["start_time"]:
                    dt = datetime.strptime(
                        f"{KAISAI_DATE} {detail['start_time']}",
                        "%Y%m%d %H:%M"
                    )
                    detail["start_time"] = dt.isoformat()

                races.append(detail)
                time.sleep(random.uniform(0.5,1.5))
            except Exception as e:
                print(f"[ERROR] {url} のパース失敗: {e}", file=sys.stderr)

        browser.close()

    if not races:
        print("[ERROR] レースデータ取得できず", file=sys.stderr)
        sys.exit(1)

    # 一括 INSERT
    errors = client.insert_rows_json(BQ_TABLE, races)
    if errors:
        print("[ERROR] BigQuery 登録エラー:", errors, file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] {len(races)} 件のレースを登録しました → {BQ_TABLE}")


if __name__ == "__main__":
    upsert_entries()