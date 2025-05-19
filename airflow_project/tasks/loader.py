import json
import os
import ast
import psycopg2
from psycopg2.extras import Json
from datetime import datetime
from json import JSONDecodeError

def loader(parsed_data, table_name: str = "odds", db_config: dict = None):
    """
    Generic loader: parsed_data can be
      - a JSON string (double-quoted) representing a list of dicts
      - a Python repr string (single-quoted) representing a list/dict
      - a native Python list of dicts
      - a native Python dict (will be wrapped into a list)
    """
    # 1) normalize rows into a Python list
    print(parsed_data)
    if isinstance(parsed_data, str):
        try:
            rows = json.loads(parsed_data)
        except JSONDecodeError:
            try:
                rows = ast.literal_eval(parsed_data)
            except (ValueError, SyntaxError) as e:
                raise ValueError(
                    "Could not parse `parsed_data` as JSON or Python literal"
                ) from e
    elif isinstance(parsed_data, dict):
        rows = [parsed_data]
    elif isinstance(parsed_data, list):
        rows = parsed_data
    else:
        raise ValueError(
            f"`parsed_data` must be JSON str, repr str, dict, or list; got {type(parsed_data)}"
        )

    # 2) DB connection setup
    default_config = {
        "host":     os.environ.get("PG_HOST", "postgres"),
        "database": os.environ.get("PG_DATABASE", "airflow"),
        "user":     os.environ.get("PG_USER", "airflow"),
        "password": os.environ.get("PG_PASSWORD", "airflow"),
        "port":     os.environ.get("PG_PORT", 5432),
    }
    cfg = db_config or default_config

    conn = psycopg2.connect(
        host=cfg["host"],
        database=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
        port=cfg.get("port", 5432)
    )

    try:
        # 3) Actual translation of rows into SQL inserts
        cur = conn.cursor()
        now = datetime.now()

        print(rows)
        for row in rows:
            if not isinstance(row, dict):
                continue

            # normalize team names for easy lookup
            if "team1" in row and isinstance(row["team1"], str):
                row["team1"] = row["team1"].lower()
            if "team2" in row and isinstance(row["team2"], str):
                row["team2"] = row["team2"].lower()

            # build column list (JSON keys + insert_datetime)
            columns = list(row.keys()) + ["insert_datetime"]
            placeholders = ", ".join(["%s"] * len(columns))
            columns_sql   = ", ".join(columns)
            sql = f"""
                INSERT INTO {table_name} ({columns_sql})
                VALUES ({placeholders})
                ON CONFLICT (team1, team2, game_time) DO NOTHING
            """

            values = []
            for v in row.values():
                if isinstance(v, (dict, list)):
                    values.append(Json(v))
                else:
                    values.append(v)
            values.append(now)

            cur.execute(sql, values)

        conn.commit()
        print(f"Loaded {len(rows)} rows into {table_name}")

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()
