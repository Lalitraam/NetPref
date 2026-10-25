import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

from config import (
    CHARTS_DIR,
    DATA_DIR,
    HBASE_DB,
    METRICS_PATH,
    MODEL_DIR,
    REPORTS_DIR,
    ROOT_DIR,
    SCORED_FLOWS_DIR,
    THRESHOLDS_PATH,
    WAREHOUSE_DB,
    get_kafka_logs_dir,
)

KAFKA_LOGS = get_kafka_logs_dir()
DBS = {
    "warehouse": WAREHOUSE_DB,
    "hbase": HBASE_DB,
}

pd.set_option("display.width", 220)
pd.set_option("display.max_columns", 40)
pd.set_option("display.max_colwidth", 38)

LOCATIONS = [
    ("Scored flows (streaming output)", SCORED_FLOWS_DIR),
    ("HBase-style store (flows, alerts)", DBS["hbase"]),
    ("Warehouse (fact_flows, v_alerts)", DBS["warehouse"]),
    ("Charts (PNG)", CHARTS_DIR),
    ("SQL reports (CSV)", REPORTS_DIR),
    ("Trained ML model", MODEL_DIR),
    ("Model metrics", METRICS_PATH),
    ("Cleaned train data", DATA_DIR / "clean_train.parquet"),
    ("Cleaned test data", DATA_DIR / "clean_test.parquet"),
    ("Label thresholds", THRESHOLDS_PATH),
    ("Kafka raw events (topic 'flows')", KAFKA_LOGS),
]


def section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def human(n):
    n = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.1f} {unit}"
        n /= 1024


def size_of(p):
    if p.is_file():
        return p.stat().st_size, 1
    files = [f for f in p.rglob("*") if f.is_file()]
    return sum(f.stat().st_size for f in files), len(files)


def show_locations():
    section("1. WHERE EVERYTHING IS STORED")
    rows = []
    for name, p in LOCATIONS:
        if p.exists():
            size, nfiles = size_of(p)
            rows.append({"what": name, "path": str(p),
                         "size": human(size), "files": nfiles})
        else:
            rows.append({"what": name, "path": str(p),
                         "size": "MISSING", "files": 0})
    with pd.option_context("display.max_colwidth", None):
        print(pd.DataFrame(rows).to_string(index=False))


def show_db(label, path):
    section(f"2. DATABASE: {label}")
    print(f"File: {path}")
    if not path.exists():
        print("File not found - run the pipeline step that creates it.")
        return
    con = duckdb.connect(str(path), read_only=True)
    tables = con.execute(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    for name, ttype in tables:
        n = con.execute(f'SELECT COUNT(*) FROM "{name}"').fetchone()[0]
        print(f"\n--- {ttype.lower()} '{name}': {n} rows ---")
        cols = con.execute(f'DESCRIBE "{name}"').df()
        print("Columns: " + ", ".join(
            f"{r.column_name} ({r.column_type})" for r in cols.itertuples()))
        print("Sample rows:")
        print(con.execute(f'SELECT * FROM "{name}" LIMIT 3').df().to_string(index=False))

    if label.startswith("HBase"):
        print()
        for t in ("flows", "alerts"):
            r = con.execute(
                f"SELECT COUNT(DISTINCT row_key), COUNT(*) FROM {t}").fetchone()
            print(f"HBase table '{t}': {r[0]} rows (row keys), {r[1]} cells")
        key = con.execute(
            "SELECT row_key FROM flows ORDER BY row_key LIMIT 1").fetchone()[0]
        print(f"\nAll cells of one row ({key}):")
        print(con.execute(
            "SELECT family, qualifier, value FROM flows WHERE row_key = ?",
            [key]).df().to_string(index=False))
    con.close()


def show_parquet():
    section("3. STREAMING OUTPUT (parquet): output/scored_flows")
    p = SCORED_FLOWS_DIR
    if not p.exists():
        print("Not found - run stream_job.py first.")
        return
    df = pd.read_parquet(p)
    print(f"{len(df)} rows, {df['flow_id'].nunique()} distinct flows")
    print("Columns: " + ", ".join(df.columns))
    print(df.head(3).to_string(index=False))


def show_model():
    section("4. MODEL RESULTS (models/metrics.json)")
    p = METRICS_PATH
    if not p.exists():
        print("Not found - run train_model.py first.")
        return
    m = json.loads(p.read_text())
    for k in ("auc_roc", "auc_pr", "accuracy", "precision_degraded",
              "recall_degraded", "f1_degraded"):
        print(f"  {k:20s} {m[k]:.4f}")
    print("  confusion:", m["confusion"])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql", help="run your own SQL query and print the result")
    parser.add_argument("--db", choices=list(DBS), default="warehouse",
                        help="database for --sql (warehouse or hbase)")
    args = parser.parse_args()

    if args.sql:
        path = DBS[args.db]
        try:
            con = duckdb.connect(str(path), read_only=True)
            print(con.execute(args.sql).df().to_string(index=False))
            con.close()
        except Exception as e:
            print("SQL error:", e)
        return

    show_locations()
    show_db("Warehouse", DBS["warehouse"])
    show_db("HBase-style store", DBS["hbase"])
    show_parquet()
    show_model()


if __name__ == "__main__":
    main()