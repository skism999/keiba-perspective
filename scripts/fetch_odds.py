import requests
from bs4 import BeautifulSoup
from typing import Dict
from datetime import datetime

def parse_odds_from_jra(race_id: str) -> Dict[int, float]:
    # JRA公式オッズページのURLと解析は適宜調整
    url = f"https://www.jra.go.jp/JRADB/accessO.html?CNAME=pw{race_id}"
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, "html.parser")
    odds = {}
    for row in soup.select("table.oddsTable tr")[1:]:
        cols = row.find_all("td")
        horse_no = int(cols[0].text)
        odds[horse_no] = float(cols[1].text)
    return odds

def parse_odds_from_netkeiba(race_id: str) -> Dict[int, float]:
    url = f"https://race.netkeiba.com/race/odds.html?race_id={race_id}"
    res = requests.get(url)
    res.encoding = 'utf-8'
    soup = BeautifulSoup(res.text, "html.parser")
    odds = {}
    for row in soup.select("table.K_ONODDS01 tr")[1:]:
        cols = row.find_all("td")
        horse_no = int(cols[0].text)
        odds[horse_no] = float(cols[2].text)
    return odds

def fetch_odds(race_id: str) -> list[dict]:
    jra = parse_odds_from_jra(race_id)
    net = parse_odds_from_netkeiba(race_id)
    results = []
    now = datetime.now()
    for horse_no in set(jra) & set(net):
        o1 = jra[horse_no]
        o2 = net[horse_no]
        results.append({
            "race_id": race_id,
            "horse_no": horse_no,
            "snapshot_at": now,
            "odds_jra": o1,
            "odds_netkeiba": o2,
            "odds_avg": (o1 + o2) / 2,
        })
    return results
