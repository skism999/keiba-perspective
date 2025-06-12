import argparse
import pandas as pd
from google.cloud import bigquery

def calc_fluctuation(race_id: str):
    client = bigquery.Client()
    sql = f"""
      SELECT horse_no, label, odds_avg
      FROM `{client.project}.jra_odds.odds_snapshot`
      WHERE race_id = '{race_id}'
    """
    df = client.query(sql).to_dataframe()
    pivot = df.pivot(index="horse_no", columns="label", values="odds_avg")
    out = []
    for hn, row in pivot.iterrows():
        for frm, to in [("1h_before","30m_before"), ("30m_before","5m_before"), ("5m_before","post_race")]:
            if pd.notna(row.get(frm)) and pd.notna(row.get(to)):
                diff = row[to] - row[frm]
                rate = diff / row[frm] * 100
                out.append({
                    "race_id": race_id,
                    "horse_no": hn,
                    "from_label": frm,
                    "to_label": to,
                    "fluctuation_value": diff,
                    "fluctuation_rate": rate
                })
    if out:
        errors = client.insert_rows_json(f"{client.project}.jra_odds.odds_fluctuation", out)
        if errors:
            print("Fluctuation insert errors:", errors)
        else:
            print(f"Inserted {len(out)} fluctuation rows for {race_id}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--race_id", required=True)
    args = parser.parse_args()
    calc_fluctuation(args.race_id)
