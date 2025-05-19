import os
import requests
from airflow.models import Variable
from datetime import datetime

def extractor_api(**kwargs):
    """
    Fetch all MLB (sport_id=9) prematch odds from Pinnacle via RapidAPI
    and return a list of dicts:
      [
        {
          "event_id": 1609500961,
          "game_time": "2025-05-16T21:10:00",
          "team1": "Cincinnati Reds",
          "odds1":  ?,    # home price
          "team2": "Cleveland Guardians",
          "odds2": ?,     # away price
          "source": "pinnacle"
        },
        ...
      ]
    """
    # RapidAPI credentials
    api_key  = Variable.get("RAPIDAPI_KEY")
    api_host = Variable.get("RAPIDAPI_HOST")
    if not api_key or not api_host:
        raise RuntimeError("RAPIDAPI_KEY and RAPIDAPI_HOST must be set in Airflow Variables")

    headers = {
        "x-rapidapi-key": api_key,
        "x-rapidapi-host": api_host,
    }
    url = "https://pinnacle-odds.p.rapidapi.com/kit/v1/markets"
    params = {
        "event_type":    "prematch",
        "sport_id":      "9",     # Baseball
        "is_have_odds":  "true"
    }

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    payload = resp.json()

    rows = []
    for event in payload.get("events", []):
        # only consider events with a money_line in the "Game" period
        money_line = (
            event.get("periods", {})
              .get("num_0", {})
              .get("money_line")
        )
        if not money_line or money_line.get("home") is None or money_line.get("away") is None:
            continue

        rows.append({
            "source":    "pinnacle",
            "game_time": event["starts"],         # ISO datetime string
            "team1":     event["home"],           # home team
            "odds1":     money_line["home"],      # home money_line
            "team2":     event["away"],           # away team
            "odds2":     money_line["away"],      # away money_line
        })

    return rows
