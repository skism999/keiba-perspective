from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import timedelta
from google.cloud import bigquery
import scripts.fetch_odds as fo
import json

def schedule_jobs():
    client = bigquery.Client()
    query = "SELECT race_id, start_time FROM `jra_odds.race` WHERE race_date = CURRENT_DATE()"
    sched = BlockingScheduler(timezone="Asia/Tokyo")

    for row in client.query(query):
        rid = row["race_id"]
        st = row["start_time"]
        for label, delta in [
            ("1h_before", timedelta(hours=1)),
            ("30m_before", timedelta(minutes=30)),
            ("5m_before", timedelta(minutes=5)),
            ("post_race", timedelta(minutes=10)),  # レース後10分後取得
        ]:
            run_at = st - delta if "before" in label else st + delta
            sched.add_job(
                func=job_fetch_store,
                trigger="date",
                run_date=run_at,
                args=[rid, label]
            )
    sched.start()

def job_fetch_store(race_id: str, label: str):
    from google.cloud import bigquery
    client = bigquery.Client()
    rows = fo.fetch_odds(race_id)
    # label フィールドを追加
    for r in rows:
        r["label"] = label
    errors = client.insert_rows_json(f"{client.project}.jra_odds.odds_snapshot", rows)
    if errors:
        print(f"Error at {race_id} {label}:", errors)
    else:
        print(f"Stored odds for {race_id} ({label}).")

if __name__ == "__main__":
    schedule_jobs()
