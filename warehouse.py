from pathlib import Path

import duckdb
import matplotlib
import pandas as pd

matplotlib.use("Agg")  # save charts to files, no window needed
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "output" / "scored_flows"
DB = ROOT / "data" / "warehouse.duckdb"
REPORTS = ROOT / "output" / "reports"
CHARTS = ROOT / "output" / "charts"
REPORTS.mkdir(parents=True, exist_ok=True)
CHARTS.mkdir(parents=True, exist_ok=True)
ALERT_PROB = 0.8

# ---------- load into the warehouse ----------
df = pd.read_parquet(SRC)
df["prediction"] = df["prediction"].astype(int)
df["event_time"] = pd.to_datetime(df["event_time"]).dt.tz_localize(None)

con = duckdb.connect(str(DB))
con.register("src_df", df)
con.execute("DROP TABLE IF EXISTS fact_flows")
con.execute("CREATE TABLE fact_flows AS SELECT * FROM src_df")
con.execute(f"""
    CREATE OR REPLACE VIEW v_alerts AS
    SELECT * FROM fact_flows
    WHERE prediction = 1 AND degraded_prob >= {ALERT_PROB}
""")
n = con.execute("SELECT COUNT(*) FROM fact_flows").fetchone()[0]
print(f"Warehouse ready: fact_flows has {n} rows -> {DB}\n")


def run(title, sql, csv_name=None):
    out = con.execute(sql).df()
    print(f"=== {title} ===")
    print(out.to_string(index=False))
    print()
    if csv_name:
        out.to_csv(REPORTS / csv_name, index=False)
    return out


# ---------- 1. link health ----------
links = run("1) Link health (worst first)", f"""
    SELECT link_id,
           COUNT(*) AS flows,
           ROUND(SUM(total_bytes) / 1e6, 2) AS mb,
           ROUND(AVG(loss_ratio), 3) AS avg_loss,
           ROUND(AVG(avg_jitter), 0) AS avg_jitter,
           SUM(prediction) AS pred_degraded,
           ROUND(100.0 * AVG(prediction), 1) AS degraded_pct,
           SUM(CASE WHEN prediction = 1 AND degraded_prob >= {ALERT_PROB}
                    THEN 1 ELSE 0 END) AS alerts
    FROM fact_flows GROUP BY link_id ORDER BY degraded_pct DESC
""", "link_health.csv")

# ---------- 2. by protocol ----------
protos = run("2) Degradation by protocol (min 20 flows)", """
    SELECT proto, COUNT(*) AS flows,
           ROUND(AVG(loss_ratio), 3) AS avg_loss,
           ROUND(AVG(avg_jitter), 0) AS avg_jitter,
           ROUND(100.0 * AVG(prediction), 1) AS degraded_pct
    FROM fact_flows GROUP BY proto HAVING COUNT(*) >= 20
    ORDER BY degraded_pct DESC LIMIT 8
""", "by_protocol.csv")

# ---------- 3. by service ----------
run("3) Degradation by service (min 20 flows)", """
    SELECT service, COUNT(*) AS flows,
           ROUND(AVG(loss_ratio), 3) AS avg_loss,
           ROUND(100.0 * AVG(prediction), 1) AS degraded_pct
    FROM fact_flows GROUP BY service HAVING COUNT(*) >= 20
    ORDER BY degraded_pct DESC LIMIT 8
""", "by_service.csv")

# ---------- 4. over time ----------
time_df = run("4) Degradation over time (10-second buckets, UTC)", """
    SELECT time_bucket(INTERVAL '10 seconds', event_time) AS bucket,
           COUNT(*) AS flows,
           ROUND(100.0 * AVG(prediction), 1) AS degraded_pct,
           ROUND(AVG(loss_ratio), 3) AS avg_loss
    FROM fact_flows GROUP BY bucket ORDER BY bucket
""", "over_time.csv")

# ---------- 5. model quality on the stream ----------
run("5) Model vs rule label on the streamed data", """
    SELECT SUM(CASE WHEN prediction = 1 AND true_degraded = 1 THEN 1 ELSE 0 END) AS tp,
           SUM(CASE WHEN prediction = 1 AND true_degraded = 0 THEN 1 ELSE 0 END) AS fp,
           SUM(CASE WHEN prediction = 0 AND true_degraded = 1 THEN 1 ELSE 0 END) AS fn,
           SUM(CASE WHEN prediction = 0 AND true_degraded = 0 THEN 1 ELSE 0 END) AS tn,
           ROUND(100.0 * AVG(CASE WHEN prediction = true_degraded THEN 1 ELSE 0 END), 1)
               AS accuracy_pct
    FROM fact_flows
""", "model_quality.csv")

# ---------- 6. riskiest flows ----------
run("6) Top 10 riskiest flows", """
    SELECT flow_id, link_id, proto, service, total_bytes,
           ROUND(loss_ratio, 3) AS loss_ratio,
           ROUND(avg_jitter, 0) AS avg_jitter,
           ROUND(degraded_prob, 3) AS degraded_prob
    FROM fact_flows ORDER BY degraded_prob DESC LIMIT 10
""", "top_risky_flows.csv")

# ---------- charts ----------
plt.rcParams.update({"figure.dpi": 130, "axes.spines.top": False,
                     "axes.spines.right": False})

fig, ax = plt.subplots(figsize=(7, 4))
d = links.sort_values("link_id")
ax.bar(d["link_id"], d["degraded_pct"], color="#3b6fb6")
ax.set_title("Share of flows predicted degraded, per link")
ax.set_ylabel("% of flows")
ax.set_xlabel("link")
for i, v in enumerate(d["degraded_pct"]):
    ax.text(i, v + 0.3, f"{v}", ha="center", fontsize=8)
fig.tight_layout()
fig.savefig(CHARTS / "degraded_by_link.png")
plt.close(fig)

fig, ax = plt.subplots(figsize=(7, 4))
p = protos.sort_values("degraded_pct")
ax.barh(p["proto"], p["degraded_pct"], color="#c0504d")
ax.set_title("Share of flows predicted degraded, by protocol")
ax.set_xlabel("% of flows")
fig.tight_layout()
fig.savefig(CHARTS / "degraded_by_protocol.png")
plt.close(fig)

fig, ax = plt.subplots(figsize=(8, 4))
labels = pd.to_datetime(time_df["bucket"]).dt.strftime("%H:%M:%S")
ax.bar(labels, time_df["degraded_pct"], color="#4f9a6b")
ax.set_title("Predicted degradation over time (10 s buckets, UTC)")
ax.set_ylabel("% of flows")
ax.tick_params(axis="x", rotation=60, labelsize=7)
fig.tight_layout()
fig.savefig(CHARTS / "degraded_over_time.png")
plt.close(fig)

print(f"CSV reports saved in:  {REPORTS}")
print(f"Charts saved in:       {CHARTS}")
con.close()