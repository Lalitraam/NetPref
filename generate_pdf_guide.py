import base64
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent

def img_to_b64(path: Path) -> str:
    if not path.exists():
        return ""
    with open(path, "rb") as f:
        data = base64.b64encode(f.read()).decode("utf-8")
    ext = path.suffix.lower().replace(".", "")
    mime = "image/png" if ext == "png" else "image/jpeg"
    return f"data:{mime};base64,{data}"

b64_dashboard = img_to_b64(ROOT / "output" / "screenshots" / "dashboard_live.png")
b64_chart_link = img_to_b64(ROOT / "output" / "charts" / "degraded_by_link.png")
b64_chart_proto = img_to_b64(ROOT / "output" / "charts" / "degraded_by_protocol.png")
b64_chart_time = img_to_b64(ROOT / "output" / "charts" / "degraded_over_time.png")

template = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>NetPref Project Guide & Demonstration Manual</title>
<style>
  @page {
    size: A4;
    margin: 14mm 14mm 14mm 14mm;
    @bottom-right {
      content: counter(page);
      font-size: 8pt;
      color: #64748b;
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
    }
  }
  *, *::before, *::after { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    color: #1e293b;
    background: #ffffff;
    line-height: 1.45;
    font-size: 9pt;
    margin: 0;
    padding: 0;
  }
  h1, h2, h3, h4 {
    color: #0f172a;
    font-weight: 700;
    margin-top: 0;
    line-height: 1.2;
  }
  h1 { font-size: 20pt; margin-bottom: 3px; }
  h2 {
    font-size: 13pt;
    margin-top: 14pt;
    margin-bottom: 6pt;
    padding-bottom: 3pt;
    border-bottom: 1.5pt solid #e2e8f0;
    color: #1e3a8a;
  }
  h3 { font-size: 10.5pt; margin-top: 10pt; margin-bottom: 3pt; color: #1e293b; }
  p { margin-top: 0; margin-bottom: 6pt; text-align: justify; }
  
  .badge {
    display: inline-block;
    padding: 2px 6px;
    font-size: 7.5pt;
    font-weight: 600;
    border-radius: 4px;
    margin-right: 4px;
    text-transform: uppercase;
  }
  .badge-primary { background: #eff6ff; color: #1d4ed8; border: 1px solid #bfdbfe; }
  .badge-success { background: #f0fdf4; color: #15803d; border: 1px solid #bbf7d0; }
  
  .callout {
    padding: 7pt 9pt;
    border-radius: 5pt;
    margin-bottom: 8pt;
    font-size: 8.5pt;
    border-left: 3.5pt solid #3b82f6;
    background: #f8fafc;
  }
  .callout-info { border-left-color: #2563eb; background: #eff6ff; }
  .callout-tip { border-left-color: #059669; background: #ecfdf5; }
  .callout-warn { border-left-color: #d97706; background: #fffbeb; }
  
  table {
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 8pt;
    font-size: 8pt;
  }
  th, td {
    padding: 4.5pt 6pt;
    text-align: left;
    border: 1px solid #e2e8f0;
  }
  th {
    background: #f1f5f9;
    color: #0f172a;
    font-weight: 600;
  }
  tr:nth-child(even) { background: #f8fafc; }
  
  code {
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 8pt;
    background: #f1f5f9;
    padding: 1px 3px;
    border-radius: 3px;
    color: #0f172a;
  }
  pre {
    font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    font-size: 7.5pt;
    background: #0f172a;
    color: #f8fafc;
    padding: 6pt 8pt;
    border-radius: 5pt;
    margin-top: 0;
    margin-bottom: 7pt;
    overflow-x: auto;
    line-height: 1.35;
  }
  
  .grid-2 {
    display: flex;
    gap: 8pt;
    margin-bottom: 8pt;
  }
  .grid-2 > div { flex: 1; }

  .grid-3 {
    display: flex;
    gap: 6pt;
    margin-bottom: 8pt;
  }
  .grid-3 > div { flex: 1; }

  .card {
    border: 1px solid #e2e8f0;
    border-radius: 5pt;
    padding: 7pt;
    background: #ffffff;
  }
  .card-header {
    font-weight: 700;
    font-size: 9pt;
    margin-bottom: 3pt;
    color: #1e3a8a;
  }
  
  .img-container {
    border: 1px solid #cbd5e1;
    border-radius: 5pt;
    overflow: hidden;
    margin-bottom: 8pt;
    background: #000;
  }
  .img-container img {
    width: 100%;
    display: block;
  }
  .img-caption {
    background: #f8fafc;
    padding: 3.5pt 6pt;
    font-size: 7.5pt;
    color: #475569;
    text-align: center;
    border-top: 1px solid #e2e8f0;
    font-weight: 500;
  }

  .page-break { page-break-before: always; }
  .avoid-break { break-inside: avoid; }

  .header-banner {
    background: linear-gradient(135deg, #0f172a 0%, #1e3a8a 100%);
    color: #ffffff;
    padding: 12pt 14pt;
    border-radius: 6pt;
    margin-bottom: 12pt;
  }
  .header-banner h1 { color: #ffffff; margin-bottom: 2pt; font-size: 18pt; }
  .header-banner p { color: #94a3b8; font-size: 9pt; margin-bottom: 5pt; text-align: left; }
  .meta-tag {
    display: inline-block;
    background: rgba(255, 255, 255, 0.15);
    color: #e2e8f0;
    padding: 2px 7px;
    border-radius: 10px;
    font-size: 7pt;
    margin-right: 5px;
  }
</style>
</head>
<body>

<!-- HEADER BANNER -->
<div class="header-banner">
  <h1>NetPref</h1>
  <p>Real-Time Network Performance & Congestion Predictor — Comprehensive System Guide</p>
  <div>
    <span class="meta-tag">Domain: Computer Networks & Big Data</span>
    <span class="meta-tag">Stack: Kafka 3.9 &bull; Spark 3.5 &bull; MLlib &bull; DuckDB &bull; Streamlit</span>
    <span class="meta-tag">Target: Computer Science Students & Engineers</span>
  </div>
</div>

<!-- SECTION 1 -->
<h2>1. What is This Project Actually About?</h2>

<p>
  In any modern computer network—from local Wi-Fi hotspots to enterprise clouds and telecommunication backbones—network congestion and performance degradation are inevitable. When traffic surges, router queue buffers fill up (a phenomenon known as <strong>bufferbloat</strong>). As buffers overflow, routers are forced to drop packets (<strong>packet loss</strong>), and packet delivery times fluctuate violently (<strong>high jitter</strong>).
</p>
<p>
  To human users and applications, this manifests as frustrating video call freezing, robotic audio, gaming latency spikes, and stalled file transfers.
</p>

<div class="grid-2">
  <div class="card">
    <div class="card-header">&#10060; The Traditional Reactive Approach</div>
    <p style="font-size:8pt; margin-bottom:0;">
      Traditional network monitoring tools calculate packet loss and jitter <em>after</em> a flow has completely finished, or by continuously sending active probe packets (ping / traceroute) that consume network bandwidth and react only after user experience is already ruined.
    </p>
  </div>
  <div class="card">
    <div class="card-header">&#9989; NetPref's Predictive Approach</div>
    <p style="font-size:8pt; margin-bottom:0;">
      <strong>NetPref</strong> observes basic, easily obtainable transport-layer behavior (connection duration, transfer rates, packet sizes, and TCP handshake timings) and uses a trained Machine Learning model to <strong>predict whether a flow is degraded in real time</strong>—without ever directly measuring packet loss or jitter!
    </p>
  </div>
</div>

<div class="callout callout-info">
  <strong>The Highway Analogy (CS Intuition):</strong> Imagine monitoring highway traffic. <em>Traditional monitoring</em> counts how many cars crashed or came to a dead halt 10 minutes ago. <em>NetPref</em> watches the subtle patterns—sudden speed drops, rapid lane weaving, and tailgating—to predict that traffic flow is degrading <em>before</em> the highway reaches total gridlock.
</div>

<h3>The Core Research Question</h3>
<p>
  <em>"Can a machine learning model accurately identify degraded network flows purely from flow behavioral signatures (duration, packet counts, inter-arrival times, window sizes, and round-trip handshake latency) without target leakage?"</em>
</p>
<p>
  NetPref proves this concept empirically on real-world network traces (UNSW-NB15), achieving <strong>97.0% classification accuracy</strong> and an <strong>AUC of 0.994</strong> while processing continuous live streams at hundreds of events per second.
</p>

<!-- SECTION 2 -->
<h2>2. Architecture & Technology Stack: What is Used for What?</h2>

<p>
  NetPref combines the standard enterprise Big Data and distributed streaming stack:
</p>

<table>
  <thead>
    <tr>
      <th style="width:20%;">Technology</th>
      <th style="width:24%;">Architectural Layer</th>
      <th>Specific Purpose & Engineering Justification</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Apache Kafka 3.9</strong><br><code>(KRaft Mode)</code></td>
      <td>Message Ingestion Buffer</td>
      <td>
        Decouples high-throughput traffic generation from downstream stream processing. Uses KRaft (Kafka Raft consensus, no ZooKeeper needed) for lightweight execution. Events are partitioned by <code>link_id</code> so traffic per link maintains strict sequential order.
      </td>
    </tr>
    <tr>
      <td><strong>Apache Spark 3.5</strong><br><code>(PySpark Streaming)</code></td>
      <td>Distributed Stream Processing</td>
      <td>
        Spark Structured Streaming processes events in micro-batches (every 5 seconds). It scales horizontally, handles backpressure, deserializes JSON, watermarks out-of-order data, and applies stateful tumbling window aggregations.
      </td>
    </tr>
    <tr>
      <td><strong>Spark MLlib</strong><br><code>(Random Forest)</code></td>
      <td>Machine Learning Inference</td>
      <td>
        Applies a pre-trained Random Forest (50 trees, max depth 8) inside the streaming DAG. Because MLlib runs natively inside the Spark JVM, it scores thousands of flows per second in parallel without Python-IPC bottlenecks.
      </td>
    </tr>
    <tr>
      <td><strong>HBase-Style Store</strong><br><code>(HBaseLite over DuckDB)</code></td>
      <td>Operational Key-Value Store</td>
      <td>
        Emulates an Apache HBase wide-column store with compound row keys (<code>link_id#reversed_ts#flow_id</code>) and column families (<code>flow:</code>, <code>pred:</code>, <code>alert:</code>). Reversed timestamps ensure that a prefix scan of a link returns the newest events first.
      </td>
    </tr>
    <tr>
      <td><strong>DuckDB Warehouse</strong><br><code>(warehouse.duckdb)</code></td>
      <td>Analytical Data Warehouse</td>
      <td>
        High-speed columnar OLAP engine for SQL reporting. Stores the <code>fact_flows</code> table and <code>v_alerts</code> view, powering slicing-and-dicing by protocol, service, time, and link health without requiring a heavy DBMS server.
      </td>
    </tr>
    <tr>
      <td><strong>Streamlit & Altair</strong><br><code>(dashboard.py)</code></td>
      <td>Interactive Visual NOC</td>
      <td>
        Web dashboard displaying real-time link health cards, alert feeds, model quality comparisons, interactive Altair charts, and an in-browser SQL query runner for deep exploration.
      </td>
    </tr>
  </tbody>
</table>

<!-- page-break removed -->

<!-- SECTION 3 -->
<h2>3. Input &rarr; Process &rarr; Output: Deep Dive</h2>

<p>
  To truly understand the system, trace the journey of a single network flow record from raw data to an executive alert:
</p>

<!-- PIPELINE FLOW CHART -->
<div class="card avoid-break" style="margin-bottom:10pt; background:#f8fafc;">
  <div style="font-weight:700; color:#1e3a8a; margin-bottom:4pt; font-size:9pt;">The End-to-End Data Lifecycle</div>
  <pre style="margin:0; font-size:7pt; background:#0f172a;">
[Input: Raw Dataset]              [Kafka: flows Topic]             [Spark Structured Streaming]
UNSW-NB15 Network Flows  ===>  JSON Record keyed by link  ===>  Schema Deserialization + Event Time
(Duration, Bytes, RTT, etc)   (Rate: 100-200 events/sec)       MLlib Pipeline: OHE + VectorAssembler
                                                                        |
                                                                        v
[Live Dashboard & Reports]         [Multi-Tier Persistence]     [Scoring & Window Aggregations]
Streamlit Interactive UI   <===  1. Parquet Scored Flows  <===  Random Forest Evaluates P(degraded)
Live Alerts & Link Stats         2. HBase-style Key-Value       Tumbling 10s Window Aggregations
DuckDB SQL Analytics             3. DuckDB Analytical WH        Alert Threshold: P(degraded) >= 0.80
  </pre>
</div>

<h3>A. What is the Input?</h3>
<p>
  The system ingests records from the <strong>UNSW-NB15</strong> benchmark network dataset. In computer networks, a <em>flow</em> is a sequence of packets between a source IP and destination IP using a specific transport protocol and ports (the 5-tuple).
</p>
<p>
  Each input record contains <strong>36 network metrics</strong>, including:
</p>
<ul>
  <li><strong>General Flow Statistics:</strong> <code>dur</code> (connection duration in seconds), <code>spkts</code>/<code>dpkts</code> (packets sent by source/destination), <code>sbytes</code>/<code>dbytes</code> (bytes sent), <code>rate</code> (overall transfer rate).</li>
  <li><strong>Timing & Dynamics:</strong> <code>sinpkt</code>/<code>dinpkt</code> (mean inter-packet arrival time in milliseconds), <code>smean</code>/<code>dmean</code> (average packet size in bytes).</li>
  <li><strong>TCP Handshake Metrics:</strong> <code>tcprtt</code> (round-trip time), <code>synack</code> (SYN-to-SYN-ACK latency), <code>ackdat</code> (SYN-ACK-to-ACK latency), <code>swin</code>/<code>dwin</code> (TCP window advertisements).</li>
</ul>

<div class="callout callout-warn">
  <strong>Critical Engineering Safeguard (Zero Target Leakage):</strong>
  The training script (<code>prepare_data.py</code>) derives the ground-truth "degraded" label using loss ratio (packet drops &gt; 85th percentile) and jitter (&gt; 90th percentile). However, <strong>these loss and jitter columns are strictly excluded from model features</strong>! The model never sees them; it must predict degradation solely from behavioral characteristics like timing, rates, and window sizes.
</div>

<h3>B. What is the Process?</h3>
<ol>
  <li><strong>Replay & Serialization (<code>producer.py</code>):</strong> Replays test flows into Kafka topic <code>flows</code>. Each flow is tagged with a simulated link ID (<code>link-1</code> to <code>link-8</code>) and stamped with <code>event_ts_ms</code> (epoch milliseconds).</li>
  <li><strong>Streaming Consumption (<code>stream_job.py</code>):</strong> Spark subscribes to Kafka with <code>maxOffsetsPerTrigger=500</code>, deserializing the JSON string into strongly-typed Spark DataFrame columns and generating an <code>event_time</code> timestamp.</li>
  <li><strong>ML Inference Pipeline:</strong> Features pass through <code>StringIndexer</code> & <code>OneHotEncoder</code> (for categorical columns <code>proto</code>, <code>service</code>, <code>state</code>), assembled into a dense vector with <code>VectorAssembler</code>, and evaluated by the 50-tree Random Forest.</li>
  <li><strong>Dual Stream Outputs:</strong>
    <ul>
      <li><em>Stream 1 (Scored Flows):</em> Every flow is enriched with <code>prediction</code> (0 or 1) and <code>degraded_prob</code> (0.0 to 1.0), written as append-only Parquet.</li>
      <li><em>Stream 2 (Windowed Link Aggregates):</em> A 10-second tumbling window groups traffic by <code>(window, link_id)</code> with a 30-second watermark, calculating average loss, mean jitter, and degraded flow rate.</li>
    </ul>
  </li>
  <li><strong>HBase Key-Value Indexing (<code>hbase_layer.py</code>):</strong> Structures data for instant sub-millisecond retrieval using a compound row key:
    <div style="text-align:center; margin: 4pt 0;">
      <code>link_id</code> # <code>(9999999999999 - timestamp_ms)</code> # <code>flow_id</code>
    </div>
    Because lexicographical sorting arranges smaller numbers first, <strong>reversing the timestamp guarantees that the newest flow on any link always sorts to the top</strong>!
  </li>
</ol>

<h3>C. What is the Output to Understand?</h3>
<ul>
  <li><strong>Degraded Probability (P(degraded)):</strong> The calibrated probability that this flow is suffering degraded performance.</li>
  <li><strong>Real-Time Alert Feed:</strong> Flows where P(degraded) &ge; 0.80 trigger alerts, alerting network operators before catastrophic buffer overflow.</li>
  <li><strong>Per-Link Degradation Ratio (%):</strong> Percentage of active traffic flagged as degraded on each link, immediately identifying bottleneck transit routes.</li>
  <li><strong>Confusion Matrix & Accuracy:</strong> Verified empirical validation against the held-out test set (97.0% Accuracy, 95.2% Precision, 92.6% Recall).</li>
</ul>

<!-- page-break removed -->

<!-- SECTION 4 -->
<h2>4. How to Run & Give a Live Demonstration (With Visuals)</h2>

<p>
  Follow this exact step-by-step procedure to execute NetPref locally and deliver a compelling, professional technical demo.
</p>

<h3>Step 1: Environment & Prerequisites</h3>
<p>Ensure you have Python 3.11, Java 17, and Apache Kafka installed. Activate your virtual environment:</p>
<pre>source .venv/bin/activate
pip install -r requirements.txt
python test_spark.py          # Verifies PySpark and Java 17 connectivity</pre>

<h3>Step 2: One-Time Data Prep & Model Training</h3>
<pre>python prepare_data.py        # Cleans dataset, calculates quantile thresholds -> data/
python train_model.py         # Trains Spark MLlib Random Forest -> models/rf_congestion/</pre>

<h3>Step 3: Launch the 4 Components in Separate Terminals</h3>

<table>
  <thead>
    <tr>
      <th style="width:18%;">Terminal</th>
      <th style="width:32%;">Command</th>
      <th>Expected Output / Ready State</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>Terminal 1: Kafka</strong></td>
      <td><code>kafka-server-start .../server.properties</code></td>
      <td>Kafka broker listens on <code>localhost:9092</code>.</td>
    </tr>
    <tr>
      <td><strong>Terminal 2: Spark Job</strong></td>
      <td><code>python stream_job.py --seconds 300</code></td>
      <td>Prints <code>Streaming started... Waiting for Kafka events</code>.</td>
    </tr>
    <tr>
      <td><strong>Terminal 3: Dashboard</strong></td>
      <td><code>streamlit run dashboard.py</code></td>
      <td>Opens browser at <code>http://localhost:8501</code> (Live monitor tab).</td>
    </tr>
    <tr>
      <td><strong>Terminal 4: Producer</strong></td>
      <td><code>python producer.py --rate 100 --limit 2000</code></td>
      <td>Streams 2,000 flows at 100 flows/sec. Micro-batches begin scoring!</td>
    </tr>
  </tbody>
</table>

<!-- SCREENSHOT 1: DASHBOARD LIVE -->
<div class="img-container avoid-break">
  <img src="__B64_DASHBOARD__" alt="Live Streamlit Dashboard">
  <div class="img-caption">
    <strong>Figure 1: Authentic Live Streamlit Monitoring Cockpit.</strong> Displays real-time metric cards (2,000 scored flows, 205 alerts, 24.7% degraded rate), per-link health bar charts with worst-link highlighting (red), tabular progress meters, and latest alert feed.
  </div>
</div>

<h3>Step 4: Load Analytical Stores (After Streaming Finishes)</h3>
<pre>python hbase_layer.py        # Ingests scored flows into HBaseLite wide-column store
python warehouse.py          # Builds fact_flows in DuckDB warehouse and generates charts
python show_outputs.py       # Summarizes all storage locations, sizes, and record counts</pre>

<!-- page-break removed -->

<!-- ANALYTICAL CHARTS SECTION -->
<h3>Analytical Warehouse Charts (Generated by DuckDB & Matplotlib)</h3>

<div class="grid-3 avoid-break">
  <div class="img-container">
    <img src="__B64_CHART_LINK__" alt="Degraded by Link">
    <div class="img-caption"><strong>Fig 2: Share per Link.</strong> Visualizes degraded ratio across all 8 monitored links.</div>
  </div>
  <div class="img-container">
    <img src="__B64_CHART_PROTO__" alt="Degraded by Protocol">
    <div class="img-caption"><strong>Fig 3: By Protocol.</strong> Compares degradation across TCP, UDP, and routing protocols.</div>
  </div>
  <div class="img-container">
    <img src="__B64_CHART_TIME__" alt="Degradation Over Time">
    <div class="img-caption"><strong>Fig 4: Over Time.</strong> 10-second tumbling time buckets showing temporal congestion spikes.</div>
  </div>
</div>

<!-- 5-MINUTE PRESENTATION SCRIPT -->
<div class="card avoid-break" style="background:#f8fafc; border-left:4px solid #1e3a8a; margin-top:8pt;">
  <div class="card-header" style="font-size:9.5pt;">&#127897; The 5-Minute Technical Pitch / Demonstration Script</div>
  <p style="font-size:8pt; margin-bottom:4pt;">
    <strong>Minute 1 — The Problem:</strong> <em>"In modern networking, packet loss and jitter kill user experience. But measuring them directly is reactive. NetPref predicts degradation in real time using only standard flow behaviors."</em>
  </p>
  <p style="font-size:8pt; margin-bottom:4pt;">
    <strong>Minute 2 — The Live Stream:</strong> Point to <strong>Figure 1</strong>: <em>"Here is our live pipeline. Producer pushes events into Kafka. Spark Structured Streaming ingests 500 events per micro-batch, applies our Random Forest model, and flags flows with P(degraded) &ge; 0.80 as critical alerts."</em>
  </p>
  <p style="font-size:8pt; margin-bottom:4pt;">
    <strong>Minute 3 — Per-Link Telemetry:</strong> Show the link health cards: <em>"Notice how each link is continuously monitored. In the detail table, we see flows, loss ratio, average jitter, and total megabytes. Link-5 is automatically highlighted in red because its degradation rate spiked to 26.5%."</em>
  </p>
  <p style="font-size:8pt; margin-bottom:4pt;">
    <strong>Minute 4 — Big Data Storage:</strong> Click the <strong>Data Explorer</strong> tab: <em>"We use a dual-store architecture. High-speed operational lookups go to our HBase-style wide-column store where row keys use reversed timestamps for fast chronological scans. Historical SQL analytics query our DuckDB warehouse."</em>
  </p>
  <p style="font-size:8pt; margin-bottom:0;">
    <strong>Minute 5 — Model Performance:</strong> Click the <strong>Analytics</strong> tab: <em>"Our model scores 97.0% accuracy and 0.994 AUC. The top predictive features are connection duration, packet counts, TCP window sizes, and handshake latency—proving that network degradation leaves unmistakable behavioral fingerprints."</em>
  </p>
</div>

<!-- SECTION 5 -->
<h2>5. How This Project Helps & Where Does It Actually Help?</h2>

<p>
  NetPref addresses real-world operational challenges across multiple telecommunications and computer systems domains:
</p>

<div class="grid-2">
  <div class="card avoid-break">
    <div class="card-header">&#127758; Internet Service Providers (ISPs) & Telecoms</div>
    <p style="font-size:8pt; margin-bottom:0;">
      ISPs manage thousands of transit links. NetPref enables <strong>predictive traffic engineering</strong>: when predictive degraded ratios rise on a trunk link, Software-Defined Network (SDN) controllers can automatically reroute non-critical traffic to secondary links <em>before</em> buffer overflow occurs.
    </p>
  </div>
  <div class="card avoid-break">
    <div class="card-header">&#9729; Cloud Data Centers & Multi-Tenancy</div>
    <p style="font-size:8pt; margin-bottom:0;">
      In AWS/GCP virtualized networks, "noisy neighbor" virtual machines can flood shared virtual switches. NetPref identifies degraded flows and pinpoints which tenant flows are suffering latency collapse without invading payload privacy.
    </p>
  </div>
</div>

<div class="grid-2" style="margin-top:6pt;">
  <div class="card avoid-break">
    <div class="card-header">&#127916; Real-Time Streaming & WebRTC (Zoom, Netflix)</div>
    <p style="font-size:8pt; margin-bottom:0;">
      Video conferencing algorithms adjust video bitrates based on packet loss. NetPref can signal impending degradation seconds ahead of time, allowing encoders to downscale resolution smoothly rather than freezing mid-call.
    </p>
  </div>
  <div class="card avoid-break">
    <div class="card-header">&#128737; Self-Healing & Autonomous Networks</div>
    <p style="font-size:8pt; margin-bottom:0;">
      Forms the telemetric foundation for AIOps (Artificial Intelligence for IT Operations). Predictive alerts can trigger automated Quality of Service (QoS) bandwidth shaping, rate-limiting, and firewall policy adjustments without human intervention.
    </p>
  </div>
</div>

<div class="callout callout-tip" style="margin-top:8pt;">
  <strong>Summary Takeaway:</strong> NetPref transforms network performance monitoring from a <em>post-mortem diagnostic chore</em> into a <em>proactive, real-time intelligence capability</em> using modern distributed big-data architecture.
</div>

</body>
</html>
"""

html_content = template.replace("__B64_DASHBOARD__", b64_dashboard) \
                       .replace("__B64_CHART_LINK__", b64_chart_link) \
                       .replace("__B64_CHART_PROTO__", b64_chart_proto) \
                       .replace("__B64_CHART_TIME__", b64_chart_time)

html_path = ROOT / "output" / "NetPref_Guide.html"
pdf_path = ROOT / "NetPref_Project_Guide.pdf"

html_path.write_text(html_content, encoding="utf-8")
print(f"Saved HTML to {html_path}")

cmd = [
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "--headless",
    "--disable-gpu",
    "--run-all-compositor-stages-before-draw",
    f"--print-to-pdf={pdf_path}",
    "--no-pdf-header-footer",
    str(html_path.as_uri()),
]

print("Rendering PDF via Google Chrome headless...")
res = subprocess.run(cmd, capture_output=True, text=True)
if res.returncode == 0 and pdf_path.exists():
    size_kb = pdf_path.stat().st_size / 1024
    print(f"SUCCESS: Generated {pdf_path} ({size_kb:.1f} KB)")
else:
    print(f"ERROR: {res.stderr}")
