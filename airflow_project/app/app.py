import os
from flask import Flask, render_template
import psycopg2
import pandas as pd

app = Flask(__name__)

# e.g. "postgresql://airflow:airflow@postgres:5432/airflow"
DATABASE_URL = os.getenv("DATABASE_URL", 
    "postgresql://airflow:airflow@localhost:5432/airflow"
)

def get_db_conn():
    return psycopg2.connect(DATABASE_URL)

@app.route("/")
def index():
    # ─── fetch all odds rows ───────────────────────────────────────────────
    conn = get_db_conn()
    df = pd.read_sql("""
        SELECT source,
               game_time,
               team1,
               odds1,
               team2,
               odds2
          FROM odds
    """, conn, parse_dates=["game_time"])
    conn.close()
    # ─── clean odds columns ────────────────────────────────────────────────
    # coerce non-float strings (e.g. "Susp") into NaN, then drop them
    df["odds1"] = pd.to_numeric(df["odds1"], errors="coerce")
    df["odds2"] = pd.to_numeric(df["odds2"], errors="coerce")
    df = df.dropna(subset=["odds1", "odds2"])

    # ─── split out each source and rename columns ──────────────────────────
    tab = (
        df[df.source == "tab"]
        .rename(columns={"odds1":"tab_odds1","odds2":"tab_odds2"})
        .drop(columns="source")
    )
    pin = (
        df[df.source == "pinnacle"]
        .rename(columns={"odds1":"pin_odds1","odds2":"pin_odds2"})
        .drop(columns="source")
    )

    # ─── inner join on game_time + teams to keep only full matches ────────
    merged = pd.merge(
        tab, pin,
        on=["game_time","team1","team2"],
        how="inner"
    )

    # ─── vectorized de-vig + EV calc ──────────────────────────────────────
    raw1 = 1 / merged["pin_odds1"]
    raw2 = 1 / merged["pin_odds2"]
    margin = raw1 + raw2
    fair1 = 1 / (raw1 / margin)
    fair2 = 1 / (raw2 / margin)

    merged["ev1"] = (merged["tab_odds1"] / fair1 - 1) * 100
    merged["ev2"] = (merged["tab_odds2"] / fair2 - 1) * 100
    merged["fair1"] = fair1
    merged["fair2"] = fair2

    # ─── explode into one‐row‐per‐side for the template ───────────────────
    rows = []
    for _, r in merged.iterrows():
        match_str = f"{r.team1} @ {r.team2} {r.game_time.strftime('%Y-%m-%d %H:%M')}"
        rows.append({
            "match": match_str,
            "tab":   f"{r.tab_odds1:.2f}",
            "fair":  f"{r.fair1:.2f}",
            "ev":    r.ev1,
            "side":  r.team1
        })
        rows.append({
            "match": match_str,
            "tab":   f"{r.tab_odds2:.2f}",
            "fair":  f"{r.fair2:.2f}",
            "ev":    r.ev2,
            "side":  r.team2
        })

    # ─── sort & render ────────────────────────────────────────────────────
    rows.sort(key=lambda x: x["match"])
    return render_template("index.html", rows=rows)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
