import argparse
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from google.cloud import bigquery

def export_date(date_str: str):
    # 認証
    creds = Credentials.from_service_account_file(
        filename="~/creds.json",
        scopes=["https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"]
    )
    gc = gspread.authorize(creds)
    # スプレッドシート作成 or open
    sh_title = f"odds-{date_str}"
    try:
        sh = gc.open(sh_title)
    except gspread.SpreadsheetNotFound:
        sh = gc.create(sh_title)
        sh.share(None, perm_type='anyone', role='reader')

    client = bigquery.Client()
    # 競馬場一覧
    tracks = client.query(f"""
      SELECT DISTINCT track FROM `{client.project}.jra_odds.race`
      WHERE race_date = '{date_str}'
    """).to_dataframe()["track"]
    for track in tracks:
        # タブ作成
        try:
            ws = sh.worksheet(track)
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(track, rows=100, cols=20)
        # レースごとにシートに書き込み
        races = client.query(f"""
          SELECT * FROM `{client.project}.jra_odds.race`
          WHERE race_date = '{date_str}' AND track = '{track}'
          ORDER BY start_time
        """).to_dataframe()
        for _, r in races.iterrows():
            df = client.query(f"""
              SELECT e.frame_no, e.horse_no, e.weight_kg, e.horse_name, e.jockey,
                     o1.odds_avg FILTER(WHERE o1.label='1h_before') AS odds_1h,
                     o2.odds_avg FILTER(WHERE o2.label='30m_before') AS odds_30m,
                     o3.odds_avg FILTER(WHERE o3.label='5m_before') AS odds_5m,
                     o4.odds_avg FILTER(WHERE o4.label='post_race') AS odds_post,
                     f.fluctuation_value, f.fluctuation_rate,
                     rr.finish_position
              FROM `{client.project}.jra_odds.entries` e
              LEFT JOIN `jra_odds.odds_snapshot` o1 ON e.race_id=o1.race_id AND e.horse_no=o1.horse_no AND o1.label='1h_before'
              LEFT JOIN `jra_odds.odds_snapshot` o2 ON e.race_id=o2.race_id AND e.horse_no=o2.horse_no AND o2.label='30m_before'
              LEFT JOIN `jra_odds.odds_snapshot` o3 ON e.race_id=o3.race_id AND e.horse_no=o3.horse_no AND o3.label='5m_before'
              LEFT JOIN `jra_odds.odds_snapshot` o4 ON e.race_id=o4.race_id AND e.horse_no=o4.horse_no AND o4.label='post_race'
              LEFT JOIN `{client.project}.jra_odds.odds_fluctuation` f ON e.race_id=f.race_id AND e.horse_no=f.horse_no
              LEFT JOIN `{client.project}.jra_odds.race_results` rr ON e.race_id=rr.race_id AND e.horse_no=rr.horse_no
              WHERE e.race_id = '{r.race_id}'
              ORDER BY e.frame_no
            """).to_dataframe()
            # シートに書き込み
            ws.clear()
            ws.update([df.columns.values.tolist()] + df.values.tolist())

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()
    export_date(args.date)
