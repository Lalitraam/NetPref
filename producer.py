import argparse
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
from kafka import KafkaProducer

from config import DATA_DIR, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, N_LINKS

parser = argparse.ArgumentParser()
parser.add_argument("--rate", type=int, default=200, help="events per second")
parser.add_argument("--limit", type=int, default=0, help="max events (0 = all)")
parser.add_argument("--bootstrap-server", default=KAFKA_BOOTSTRAP_SERVERS,
                    help="Kafka bootstrap servers (default from config/env)")
parser.add_argument("--topic", default=KAFKA_TOPIC,
                    help="Kafka topic to send events to")
args = parser.parse_args()

# replay the TEST split (the model never trained on it), shuffled
df = (
    pd.read_parquet(DATA_DIR / "clean_test.parquet")
    .sample(frac=1.0, random_state=42)
    .reset_index(drop=True)
)
if args.limit > 0:
    df = df.head(args.limit)

rng = np.random.default_rng(42)
df["link_id"] = ["link-" + str(i) for i in rng.integers(1, N_LINKS + 1, len(df))]
# keep the rule-based label only for later comparison; the model never sees it
df = df.rename(columns={"degraded": "true_degraded"})
num_cols = df.select_dtypes(include="number").columns
df[num_cols] = df[num_cols].fillna(0)
records = df.to_dict("records")


def to_json(v):
    return json.dumps(
        v, default=lambda o: o.item() if hasattr(o, "item") else str(o)
    ).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers=args.bootstrap_server,
    value_serializer=to_json,
    key_serializer=lambda k: k.encode("utf-8"),
    linger_ms=20,
)

print(f"Sending {len(records)} events to topic '{args.topic}' at ~{args.rate}/sec "
      f"(Ctrl+C to stop)", flush=True)
sent = 0
try:
    for i in range(0, len(records), args.rate):
        t0 = time.time()
        for rec in records[i:i + args.rate]:
            rec["event_ts_ms"] = int(time.time() * 1000)  # epoch ms, timezone-safe
            producer.send(args.topic, key=rec["link_id"], value=rec)
            sent += 1
        producer.flush()
        print(f"sent {sent}", flush=True)
        time.sleep(max(0.0, 1.0 - (time.time() - t0)))
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    producer.flush()
    producer.close()
    print(f"Done. Total sent: {sent}")