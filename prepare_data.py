import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"

# Label rule: tune these if the class balance looks off
JITTER_QUANTILE = 0.90   # jitter above this training percentile = degraded
LOSS_QUANTILE = 0.85    # more than 5% packets lost = degraded

CAT_COLS = ["proto", "service", "state"]
NUM_COLS = [
    "dur", "spkts", "dpkts", "sbytes", "dbytes", "rate", "sload", "dload",
    "sinpkt", "dinpkt", "smean", "dmean", "swin", "dwin",
    "tcprtt", "synack", "ackdat", "ct_src_dport_ltm", "ct_dst_sport_ltm",
]
# Raw columns kept only for streaming window metrics (NOT model inputs)
RAW_METRIC_COLS = ["sloss", "dloss", "sjit", "djit"]


def add_metrics(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for c in ["spkts", "dpkts", "sbytes", "dbytes", "sloss", "dloss"]:
        df[c] = df[c].astype("int64")  # avoid int16 overflow when summing
    df["total_pkts"] = df["spkts"] + df["dpkts"]
    df["total_bytes"] = df["sbytes"] + df["dbytes"]
    lost = df["sloss"] + df["dloss"]
    df["loss_ratio"] = (lost / df["total_pkts"]).where(df["total_pkts"] > 0, 0.0)
    df["avg_jitter"] = (df["sjit"].astype("float64") + df["djit"].astype("float64")) / 2
    for c in CAT_COLS + ["attack_cat"]:
        df[c] = df[c].astype(str)
    return df


def add_label(df: pd.DataFrame, jitter_thr: float, loss_thr: float) -> pd.DataFrame:
    df["degraded"] = (
        (df["loss_ratio"] > loss_thr) | (df["avg_jitter"] > jitter_thr)
    ).astype("int8")
    return df


def main():
    train = add_metrics(pd.read_parquet(DATA / "UNSW_NB15_training-set.parquet"))
    test = add_metrics(pd.read_parquet(DATA / "UNSW_NB15_testing-set.parquet"))

    # thresholds come from TRAIN only, then applied to both
    jitter_thr = float(train["avg_jitter"].quantile(JITTER_QUANTILE))
    loss_thr = float(train["loss_ratio"].quantile(LOSS_QUANTILE))
    train = add_label(train, jitter_thr, loss_thr)
    test = add_label(test, jitter_thr, loss_thr)

    keep = (
        CAT_COLS + NUM_COLS + RAW_METRIC_COLS
        + ["total_pkts", "total_bytes", "loss_ratio", "avg_jitter",
           "attack_cat", "degraded"]
    )
    for name, df in [("train", train), ("test", test)]:
        out = df[keep].reset_index(drop=True)
        out.insert(0, "flow_id", out.index.astype("int64"))
        out.to_parquet(DATA / f"clean_{name}.parquet", index=False)
        print(f"{name}: {out.shape[0]} rows | degraded share = "
              f"{out['degraded'].mean():.1%}")

    with open(DATA / "thresholds.json", "w") as f:
        json.dump(
            {"jitter_threshold": jitter_thr, "loss_threshold": loss_thr}, f,
            indent=2,
        )
    print("jitter threshold:", round(jitter_thr, 2), "| loss threshold:", round(loss_thr, 4))
    print("Saved clean_train.parquet, clean_test.parquet, thresholds.json in /data")


if __name__ == "__main__":
    main()