import json
from datetime import datetime
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from config import ALERT_PROB, HBASE_DB, METRICS_PATH, SCORED_FLOWS_DIR, WAREHOUSE_DB

FLOWS_GLOB = (SCORED_FLOWS_DIR / "*.parquet").as_posix()
DBS = {
    "Warehouse (fact_flows, v_alerts)": WAREHOUSE_DB,
    "HBase-style store (flows, alerts)": HBASE_DB,
}
METRICS = METRICS_PATH
TZ = "Asia/Kolkata"  # display timezone
BLUE, RED, GREEN = "#3b6fb6", "#c0504d", "#4f9a6b"

st.set_page_config(page_title="Network Performance Monitor", page_icon="📡",
                   layout="wide")
st.markdown("<style>.block-container{padding-top:2rem;}</style>",
            unsafe_allow_html=True)
st.title("📡 Real-Time Network Performance & Congestion Predictor")
st.caption("Kafka → Spark Structured Streaming + MLlib → "
           "wide-column store + DuckDB warehouse")

with st.sidebar:
    st.header("Controls")
    auto = st.toggle("Auto-refresh every 3 s", value=True)
    thr = st.slider("Alert threshold (degraded probability)",
                    0.50, 0.95, ALERT_PROB, 0.05)
    st.caption(f"Times are shown in {TZ}.")
    st.caption("Live data: output/scored_flows (written by stream_job.py)")


# ------------------------------------------------------------------ data
@st.cache_data(ttl=2, show_spinner=False)
def load_flows():
    try:
        con = duckdb.connect()
        df = con.execute(f"SELECT * FROM read_parquet('{FLOWS_GLOB}')").df()
        con.close()
    except Exception:
        return None
    if df.empty:
        return None
    df = df.sort_values("event_time").drop_duplicates("flow_id", keep="first")
    t = pd.to_datetime(df["event_time"])
    if t.dt.tz is None:
        t = t.dt.tz_localize("UTC")
    df["event_time"] = t.dt.tz_convert(TZ).dt.tz_localize(None)
    df["prediction"] = df["prediction"].astype(int)
    return df.reset_index(drop=True)


def prepare(df, threshold):
    df = df.copy()
    df["is_alert"] = (df["prediction"] == 1) & (df["degraded_prob"] >= threshold)
    return df


def link_stats(df):
    g = df.groupby("link_id").agg(
        flows=("flow_id", "count"),
        mb=("total_bytes", lambda s: s.sum() / 1e6),
        avg_loss=("loss_ratio", "mean"),
        avg_jitter=("avg_jitter", "mean"),
        degraded=("prediction", "sum"),
        alerts=("is_alert", "sum"),
    )
    g["degraded_pct"] = 100 * g["degraded"] / g["flows"]
    return g.reset_index()


def run_query(path, sql, params=None):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute(sql, params or []).df()
    finally:
        con.close()


tab_live, tab_an, tab_ex, tab_about = st.tabs(
    ["Live monitor", "Analytics", "Data explorer", "About"])

# ------------------------------------------------------------ live monitor
with tab_live:

    @st.fragment(run_every=3 if auto else None)
    def live_view():
        raw = load_flows()
        if raw is None:
            st.info("Waiting for data... start stream_job.py, then producer.py.")
            return
        df = prepare(raw, thr)

        c1, c2, c3, c4, c5 = st.columns(5)
        with c1.container(border=True):
            st.metric("Flows scored", f"{len(df):,}")
        with c2.container(border=True):
            st.metric("Alerts", f"{int(df['is_alert'].sum()):,}",
                      help=f"Predicted degraded with probability >= {thr:.2f}")
        with c3.container(border=True):
            st.metric("Predicted degraded", f"{df['prediction'].mean() * 100:.1f}%")
        with c4.container(border=True):
            st.metric("Avg packet loss", f"{df['loss_ratio'].mean():.1%}")
        with c5.container(border=True):
            st.metric("Links monitored", int(df["link_id"].nunique()))
        st.caption(f"Latest event: {df['event_time'].max():%H:%M:%S}  |  "
                   f"page updated {datetime.now():%H:%M:%S}")

        stats = link_stats(df)
        stats["is_worst"] = stats["degraded_pct"] == stats["degraded_pct"].max()

        left, right = st.columns([1, 1])
        with left:
            st.subheader("Link health")
            chart = alt.Chart(stats).mark_bar().encode(
                x=alt.X("link_id:N", sort="-y", title=None),
                y=alt.Y("degraded_pct:Q", title="% flows predicted degraded"),
                color=alt.condition("datum.is_worst", alt.value(RED),
                                    alt.value(BLUE)),
                tooltip=["link_id", "flows",
                         alt.Tooltip("degraded_pct:Q", format=".1f"),
                         "alerts"],
            ).properties(height=300)
            st.altair_chart(chart)
        with right:
            st.subheader("Per-link detail")
            tbl = stats.sort_values("degraded_pct", ascending=False)[
                ["link_id", "flows", "degraded_pct", "alerts", "avg_loss",
                 "avg_jitter", "mb"]]
            top = float(max(30, stats["degraded_pct"].max() * 1.2))
            st.dataframe(
                tbl, hide_index=True,
                column_config={
                    "link_id": "Link",
                    "flows": "Flows",
                    "degraded_pct": st.column_config.ProgressColumn(
                        "Degraded %", min_value=0, max_value=top,
                        format="%.1f%%"),
                    "alerts": "Alerts",
                    "avg_loss": st.column_config.NumberColumn(
                        "Avg loss", format="%.3f"),
                    "avg_jitter": st.column_config.NumberColumn(
                        "Avg jitter", format="%.0f"),
                    "mb": st.column_config.NumberColumn("MB", format="%.2f"),
                },
            )

        st.subheader("Latest alerts")
        feed = (df[df["is_alert"]].sort_values("event_time", ascending=False)
                .head(12).copy())
        if feed.empty:
            st.caption("No alerts at this threshold yet.")
        else:
            feed["time"] = feed["event_time"].dt.strftime("%H:%M:%S")
            feed["loss_pct"] = feed["loss_ratio"] * 100
            st.dataframe(
                feed[["time", "link_id", "proto", "service", "loss_pct",
                      "avg_jitter", "degraded_prob"]],
                hide_index=True,
                column_config={
                    "time": "Time", "link_id": "Link", "proto": "Protocol",
                    "service": "Service",
                    "loss_pct": st.column_config.NumberColumn(
                        "Loss", format="%.1f%%"),
                    "avg_jitter": st.column_config.NumberColumn(
                        "Jitter", format="%.0f"),
                    "degraded_prob": st.column_config.ProgressColumn(
                        "Probability", min_value=0.0, max_value=1.0,
                        format="%.2f"),
                },
            )

        st.subheader("Over time (10-second buckets)")
        ts = (df.assign(bucket=df["event_time"].dt.floor("10s"))
              .groupby("bucket")
              .agg(flows=("flow_id", "count"),
                   degraded_pct=("prediction", lambda s: s.mean() * 100))
              .reset_index())
        a, b = st.columns(2)
        with a:
            st.altair_chart(
                alt.Chart(ts).mark_line(point=True, color=RED).encode(
                    x=alt.X("bucket:T", title=None),
                    y=alt.Y("degraded_pct:Q", title="% predicted degraded"),
                    tooltip=["bucket:T",
                             alt.Tooltip("degraded_pct:Q", format=".1f")],
                ).properties(height=240))
        with b:
            st.altair_chart(
                alt.Chart(ts).mark_bar(color=GREEN).encode(
                    x=alt.X("bucket:T", title=None),
                    y=alt.Y("flows:Q", title="flows per bucket"),
                    tooltip=["bucket:T", "flows"],
                ).properties(height=240))

    live_view()

# --------------------------------------------------------------- analytics
with tab_an:
    raw = load_flows()
    if raw is None:
        st.info("No streamed data yet. Run stream_job.py and producer.py first.")
    else:
        df = prepare(raw, thr)

        st.subheader("Model quality")
        tp = int(((df["prediction"] == 1) & (df["true_degraded"] == 1)).sum())
        fp = int(((df["prediction"] == 1) & (df["true_degraded"] == 0)).sum())
        fn = int(((df["prediction"] == 0) & (df["true_degraded"] == 1)).sum())
        tn = int(((df["prediction"] == 0) & (df["true_degraded"] == 0)).sum())
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        acc = (tp + tn) / max(1, tp + fp + fn + tn)

        if METRICS.exists():
            m = json.loads(METRICS.read_text())
            st.markdown("**Held-out test set (from training)**")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("AUC", f"{m['auc_roc']:.3f}")
            k2.metric("Precision", f"{m['precision_degraded']:.1%}")
            k3.metric("Recall", f"{m['recall_degraded']:.1%}")
            k4.metric("Accuracy", f"{m['accuracy']:.1%}")

        st.markdown("**Streamed data (this run)**")
        s1, s2, s3, s4 = st.columns(4)
        s1.metric("Flows", f"{len(df):,}")
        s2.metric("Precision", f"{prec:.1%}")
        s3.metric("Recall", f"{rec:.1%}")
        s4.metric("Accuracy", f"{acc:.1%}")
        cm = pd.DataFrame(
            {"Predicted OK": [tn, fn], "Predicted degraded": [fp, tp]},
            index=["Actually OK", "Actually degraded"])
        st.dataframe(cm)

        def group_chart(col, title, color):
            g = (df.groupby(col)
                 .agg(flows=("flow_id", "count"),
                      degraded_pct=("prediction", lambda s: s.mean() * 100),
                      avg_loss=("loss_ratio", "mean"))
                 .reset_index())
            g = g[g["flows"] >= 20]
            st.markdown(f"**{title}** (groups with 20+ flows)")
            if g.empty:
                st.caption("Not enough flows yet.")
                return
            st.altair_chart(
                alt.Chart(g).mark_bar(color=color).encode(
                    y=alt.Y(f"{col}:N", sort="-x", title=None),
                    x=alt.X("degraded_pct:Q", title="% flows predicted degraded"),
                    tooltip=[col, "flows",
                             alt.Tooltip("degraded_pct:Q", format=".1f"),
                             alt.Tooltip("avg_loss:Q", format=".3f")],
                ).properties(height=max(120, 34 * len(g))))

        p1, p2 = st.columns(2)
        with p1:
            group_chart("proto", "By protocol", RED)
        with p2:
            group_chart("service", "By service", BLUE)

        st.subheader("Top 10 riskiest flows")
        risky = df.sort_values("degraded_prob", ascending=False).head(10).copy()
        risky["loss_pct"] = risky["loss_ratio"] * 100
        st.dataframe(
            risky[["flow_id", "link_id", "proto", "service", "total_bytes",
                   "loss_pct", "avg_jitter", "degraded_prob"]],
            hide_index=True,
            column_config={
                "loss_pct": st.column_config.NumberColumn("Loss", format="%.1f%%"),
                "avg_jitter": st.column_config.NumberColumn(
                    "Jitter", format="%.0f"),
                "degraded_prob": st.column_config.ProgressColumn(
                    "Probability", min_value=0.0, max_value=1.0, format="%.2f"),
            },
        )

        with st.expander("How to read these results correctly"):
            st.markdown(
                "- **Links are simulated.** The dataset has no link information, "
                "so flows are assigned to 8 links at random. Differences between "
                "links are noise by design; they demonstrate per-link monitoring.\n"
                "- **Only TCP shows loss.** The dataset records packet loss for "
                "TCP only, so UDP flows are never labelled degraded.\n"
                "- **The label is a rule** (high loss or high jitter). The model "
                "does not see those columns, but some inputs correlate with them, "
                "which is part of why accuracy is high."
            )

# ------------------------------------------------------------ data explorer
with tab_ex:
    st.subheader("Browse the stored data")
    db_name = st.radio("Database", list(DBS), horizontal=True)
    path = DBS[db_name]
    if not path.exists():
        st.warning(f"{path.name} not found. Run "
                   f"{'warehouse.py' if 'Warehouse' in db_name else 'hbase_layer.py'}"
                   " first.")
    else:
        st.code(str(path), language=None)
        tables = run_query(
            path,
            "SELECT table_name, table_type FROM information_schema.tables "
            "WHERE table_schema = 'main' ORDER BY table_name")
        summary = []
        for t, ty in zip(tables["table_name"], tables["table_type"]):
            n = int(run_query(path, f'SELECT COUNT(*) AS n FROM "{t}"')["n"][0])
            summary.append({"Table": t, "Type": ty.title(), "Rows": n})
        st.dataframe(pd.DataFrame(summary), hide_index=True)

        sel = st.selectbox("Open a table", list(tables["table_name"]))
        cols = run_query(path, f'DESCRIBE "{sel}"')[["column_name", "column_type"]]
        with st.expander("Columns", expanded=False):
            st.dataframe(cols, hide_index=True)
        n_rows = st.slider("Rows to preview", 5, 200, 10)
        st.dataframe(run_query(path, f'SELECT * FROM "{sel}" LIMIT {int(n_rows)}'),
                     hide_index=True)

        if "row_key" in list(cols["column_name"]):
            st.markdown("**Row-key prefix scan** (like an HBase scan)")
            x1, x2 = st.columns([3, 1])
            prefix = x1.text_input("Row-key prefix", value="link-2#")
            n_keys = x2.number_input("Rows", min_value=1, max_value=50, value=10)
            sql = f"""
                SELECT row_key, family || ':' || qualifier AS col, value
                FROM "{sel}"
                WHERE row_key IN (
                    SELECT DISTINCT row_key FROM "{sel}"
                    WHERE starts_with(row_key, ?) ORDER BY row_key LIMIT ?)
            """
            cells = run_query(path, sql, [prefix, int(n_keys)])
            if cells.empty:
                st.caption("No rows match that prefix.")
            else:
                wide = cells.pivot_table(index="row_key", columns="col",
                                         values="value", aggfunc="first")
                st.dataframe(wide)

        st.subheader("Run your own SQL")
        default = ("SELECT link_id, COUNT(*) AS flows, "
                   "ROUND(100 * AVG(prediction), 1) AS degraded_pct "
                   "FROM fact_flows GROUP BY link_id ORDER BY degraded_pct DESC"
                   if "Warehouse" in db_name else "SELECT * FROM flows LIMIT 10")
        sql_text = st.text_area("SQL (read-only)", value=default, height=110,
                                key=f"sql_{path.stem}")
        if st.button("Run query"):
            try:
                res = run_query(path, sql_text)
                st.dataframe(res, hide_index=True)
                st.caption(f"{len(res)} rows")
            except Exception as e:
                st.error(str(e))

# ------------------------------------------------------------------- about
with tab_about:
    st.subheader("How it works")
    st.graphviz_chart(r"""
    digraph {
      rankdir=LR;
      node [shape=box, style="rounded,filled", fillcolor="#eef3fb",
            fontname="Helvetica"];
      P [label="Traffic producer\n(Python)"];
      K [label="Kafka\ntopic: flows"];
      S [label="Spark Structured\nStreaming + MLlib"];
      H [label="Wide-column store\n(HBase-style)"];
      W [label="DuckDB\nwarehouse"];
      D [label="This dashboard"];
      P -> K -> S;
      S -> H; S -> W;
      H -> D; W -> D;
    }
    """)
    st.markdown("""
1. **Producer** replays real network flow records (UNSW-NB15) as live traffic.
2. **Kafka** carries the events.
3. **Spark** groups traffic into 10-second windows per link and scores every flow
   with a Random Forest trained offline.
4. **Storage:** an HBase-style wide-column model (row key
   `link_id#reversed_timestamp#flow_id`, families `flow:`, `pred:`, `alert:`)
   and a DuckDB warehouse for SQL analytics.
5. **Dashboard** reads the saved output and the two databases.
""")
    st.markdown("**Limitations:** simulated links, loss recorded for TCP only, "
                "rule-based label, and the HBase layer is an emulation of the "
                "data model on DuckDB (real HBase is not practical on native "
                "Windows).")