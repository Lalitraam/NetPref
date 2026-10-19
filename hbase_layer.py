from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "output" / "scored_flows"
DB = ROOT / "data" / "hbase_lite.duckdb"
ALERT_PROB = 0.8
MAX_TS = 9_999_999_999_999  # for reversed timestamps (newest sorts first)

# qualifier -> column family
FLOW_CELLS = {
    "proto": "flow", "service": "flow", "state": "flow",
    "total_pkts": "flow", "total_bytes": "flow",
    "loss_ratio": "flow", "avg_jitter": "flow",
    "degraded_prob": "pred", "prediction": "pred", "true_degraded": "pred",
}
ALERT_CELLS = {
    "degraded_prob": "alert", "proto": "alert", "service": "alert",
    "total_bytes": "alert", "loss_ratio": "alert", "avg_jitter": "alert",
}


def rev_ts(ms: int) -> str:
    return f"{MAX_TS - int(ms):013d}"


class HBaseLite:
    """Tiny HBase-style wide-column store (cells: row key, family:qualifier, ts, value)."""

    def __init__(self, path):
        self.con = duckdb.connect(str(path))

    def create_table(self, name):
        self.con.execute(f"DROP TABLE IF EXISTS {name}")
        self.con.execute(
            f"CREATE TABLE {name} (row_key VARCHAR, family VARCHAR, "
            f"qualifier VARCHAR, ts BIGINT, value VARCHAR)"
        )

    def put_cells(self, name, cells: pd.DataFrame):
        self.con.register("cells_df", cells)
        self.con.execute(
            f"INSERT INTO {name} "
            f"SELECT row_key, family, qualifier, ts, value FROM cells_df"
        )
        self.con.unregister("cells_df")

    def put(self, name, row_key, data: dict, ts: int):
        rows = [(row_key, k.split(":")[0], k.split(":")[1], ts, str(v))
                for k, v in data.items()]
        self.con.executemany(f"INSERT INTO {name} VALUES (?, ?, ?, ?, ?)", rows)

    def get(self, name, row_key):
        rows = self.con.execute(
            f"SELECT family, qualifier, value FROM {name} WHERE row_key = ?",
            [row_key],
        ).fetchall()
        return {f"{f}:{q}": v for f, q, v in rows}

    def scan(self, name, start=None, stop=None, prefix=None, limit=None):
        """Rows with start <= row_key < stop, sorted by row key. prefix is shorthand."""
        if prefix:
            start = prefix
            stop = prefix[:-1] + chr(ord(prefix[-1]) + 1)
        conds, params = [], []
        if start is not None:
            conds.append("row_key >= ?")
            params.append(start)
        if stop is not None:
            conds.append("row_key < ?")
            params.append(stop)
        where = ("WHERE " + " AND ".join(conds)) if conds else ""
        keys_sql = f"SELECT DISTINCT row_key FROM {name} {where} ORDER BY row_key"
        if limit:
            keys_sql += f" LIMIT {int(limit)}"
        rows = self.con.execute(
            f"SELECT c.row_key, c.family, c.qualifier, c.value "
            f"FROM {name} c JOIN ({keys_sql}) k ON c.row_key = k.row_key "
            f"ORDER BY c.row_key",
            params,
        ).fetchall()
        out = {}
        for rk, f, q, v in rows:
            out.setdefault(rk, {})[f"{f}:{q}"] = v
        return list(out.items())

    def count_rows(self, name):
        return self.con.execute(
            f"SELECT COUNT(DISTINCT row_key) FROM {name}"
        ).fetchone()[0]


def to_cells(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    parts = []
    for qualifier, family in mapping.items():
        p = df[["row_key", "ts", qualifier]].rename(columns={qualifier: "value"})
        p = p.assign(family=family, qualifier=qualifier)
        p["value"] = p["value"].astype(str)
        parts.append(p)
    return pd.concat(parts, ignore_index=True)


def show(rows, cols):
    if not rows:
        print("  (no rows)")
        return
    data = []
    for rk, cells in rows:
        link, rts, fid = rk.split("#")
        ms = MAX_TS - int(rts)
        t = pd.to_datetime(ms, unit="ms").strftime("%H:%M:%S.%f")[:-3]
        data.append({"link": link, "time_utc": t, "flow_id": int(fid),
                     **{c: cells.get(c) for c in cols}})
    print(pd.DataFrame(data).to_string(index=False))


def main():
    df = pd.read_parquet(SRC)
    ts = pd.to_datetime(df["event_time"], utc=True)
    df["ts"] = ((ts - pd.Timestamp("1970-01-01", tz="UTC"))
                // pd.Timedelta(milliseconds=1)).astype("int64")
    df["prediction"] = df["prediction"].astype(int)
    df["row_key"] = (
        df["link_id"] + "#"
        + (MAX_TS - df["ts"]).map("{:013d}".format) + "#"
        + df["flow_id"].astype("int64").map("{:07d}".format)
    )
    df = df.drop_duplicates("row_key")

    hb = HBaseLite(DB)
    hb.create_table("flows")
    hb.create_table("alerts")
    hb.put_cells("flows", to_cells(df, FLOW_CELLS))
    alerts = df[(df["prediction"] == 1) & (df["degraded_prob"] >= ALERT_PROB)]
    hb.put_cells("alerts", to_cells(alerts, ALERT_CELLS))

    print(f"Loaded: flows table = {hb.count_rows('flows')} rows, "
          f"alerts table = {hb.count_rows('alerts')} rows")
    print(f"Saved to {DB}\n")

    # 1. GET by row key
    one = df.iloc[0]["row_key"]
    print(f"1) GET flows '{one}':")
    for k, v in sorted(hb.get("flows", one).items()):
        print(f"   {k:20s} {v}")

    # 2. SCAN by prefix = latest flows of one link (newest first)
    print("\n2) SCAN flows prefix 'link-2#' (latest 5):")
    show(hb.scan("flows", prefix="link-2#", limit=5),
         ["flow:proto", "flow:loss_ratio", "pred:degraded_prob"])

    # 3. SCAN alerts of one link
    print("\n3) SCAN alerts prefix 'link-2#' (latest 5):")
    show(hb.scan("alerts", prefix="link-2#", limit=5),
         ["alert:proto", "alert:service", "alert:degraded_prob"])

    # 4. RANGE scan = a time window for one link (last 60 s of data)
    tmax = int(df["ts"].max())
    t0 = tmax - 60_000
    start, stop = f"link-3#{rev_ts(tmax)}", f"link-3#{rev_ts(t0)}$"
    rows = hb.scan("flows", start=start, stop=stop)
    print(f"\n4) RANGE scan link-3, last 60 s of data: {len(rows)} flows "
          f"(first 3 shown)")
    show(rows[:3], ["flow:proto", "flow:total_bytes", "pred:prediction"])


if __name__ == "__main__":
    main()