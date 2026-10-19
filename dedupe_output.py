import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
SRC = ROOT / "output" / "scored_flows"
TMP = ROOT / "output" / "_dedup.parquet"

df = pd.read_parquet(SRC)
before = len(df)
df = (
    df.sort_values("event_time")
    .drop_duplicates("flow_id", keep="first")
    .reset_index(drop=True)
)
df.to_parquet(TMP, index=False)
shutil.rmtree(SRC)
SRC.mkdir(parents=True)
shutil.move(str(TMP), str(SRC / "part-0.parquet"))
print(f"Deduplicated by flow_id: {before} -> {len(df)} rows")