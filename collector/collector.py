"""
Bronze collector: Barentswatch live AIS stream -> one gzip JSONL file per FLUSH_SECONDS.

The live endpoint is a never-ending HTTP stream, so this is a long-running process
(NOT a scheduled job). It buffers messages and cuts a file at every wall-clock
boundary (default 120 s). Raw message text is kept untouched in `payload`;
metadata columns are prefixed with underscore.

Run:  python -m collector.collector
"""
import gzip, json, os, signal, sys, threading, time, uuid
from datetime import datetime, timezone
from pathlib import Path

import requests

import observability as obs
import pipeline_config

TOKEN_URL = "https://id.barentswatch.no/connect/token"
STREAM_URL = "https://live.ais.barentswatch.no/v1/combined"
SOURCE = "barentswatch_live_ais_v1_combined"

CLIENT_ID = os.environ.get("BW_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("BW_CLIENT_SECRET", "")
CFG = pipeline_config.load()
BRONZE_DIR = Path(pipeline_config.path(CFG, "bronze/raw"))
METRICS_DIR = Path(CFG["metrics_dir"])
FLUSH_SECONDS = int(CFG["flush_seconds"])

RUN_ID = uuid.uuid4().hex[:12]
lock = threading.Lock()
buffer: list = []          # (ingest_ts_iso, raw_line)
stop = threading.Event()


def utcnow():
    return datetime.now(timezone.utc)


def get_token():
    r = requests.post(TOKEN_URL, data={
        "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET,
        "scope": "ais", "grant_type": "client_credentials"}, timeout=30)
    r.raise_for_status()
    j = r.json()
    return j["access_token"], time.time() + int(j.get("expires_in", 3600)) - 120


def connect(attempt):
    """Token + open the stream. Traced as one short span; a failure marks it as an error."""
    with obs.span("bronze.connect", attempt=attempt, source=SOURCE):
        token, expiry = get_token()
        resp = requests.get(STREAM_URL, params={"modelType": "Full", "modelFormat": "Json"},
                            headers={"Authorization": f"Bearer {token}"},
                            stream=True, timeout=(15, 90))
        try:
            resp.raise_for_status()
        except Exception:
            resp.close()
            raise
        obs.annotate(http_status=resp.status_code)
        return resp, expiry


def reader():
    """Connect, read lines, reconnect with backoff; refresh token before expiry."""
    backoff, attempt = 1, 0
    while not stop.is_set():
        attempt += 1
        try:
            resp, expiry = connect(attempt)
            backoff, started, n, reason = 1, time.time(), 0, "stream_closed"
            with resp:
                for line in resp.iter_lines():
                    if stop.is_set():
                        reason = "stopping"
                        break
                    if time.time() > expiry:
                        reason = "token_refresh"
                        break           # outer loop reconnects with a fresh token
                    if line:
                        n += 1
                        with lock:
                            buffer.append((utcnow().isoformat(), line.decode("utf-8", "replace")))
            # A stream lasts up to an hour, so it is not one long span: a short span records how it ended.
            with obs.span("bronze.stream_end", reason=reason, messages=n,
                          duration_s=round(time.time() - started, 1), attempt=attempt):
                pass
        except Exception as e:
            log_event("reader_error", error=repr(e), backoff=backoff)
            with obs.span("bronze.reader_error", error=repr(e)[:300], backoff_s=backoff, attempt=attempt):
                obs.mark_error(type(e).__name__)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


def make_record(ingest_ts, line, file_name, run_id=None):
    """Bronze record contract: raw payload untouched + lineage metadata."""
    return {"_source": SOURCE, "_ingest_ts": ingest_ts, "_run_id": run_id or RUN_ID,
            "_file": file_name, "payload": line}


def log_event(kind, **kw):
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    rec = {"ts": utcnow().isoformat(), "run_id": RUN_ID, "event": kind, **kw}
    with open(METRICS_DIR / "events.jsonl", "a") as f:
        f.write(json.dumps(rec) + "\n")


def flush(window_start, window_end):
    with obs.span("bronze.flush", source=SOURCE, run_id=RUN_ID):
        _flush(window_start, window_end)


def _flush(window_start, window_end):
    with lock:
        batch = buffer[:]
        buffer.clear()
    secs = (window_end - window_start).total_seconds()
    if not batch:
        log_event("flush", file=None, messages=0, bytes_raw=0, bytes_gz=0, window_s=secs)
        obs.annotate(messages=0, window_s=secs)
        obs.event("empty_window")
        return
    d = BRONZE_DIR / f"dt={window_end:%Y-%m-%d}" / f"hour={window_end:%H}"
    d.mkdir(parents=True, exist_ok=True)
    name = f"ais_{RUN_ID}_{window_end:%Y%m%dT%H%M%SZ}.jsonl.gz"
    tmp = d / f".tmp_{name}"            # Spark ignores dot-files => no half-written reads
    raw_bytes = 0
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        for ts, line in batch:
            s = json.dumps(make_record(ts, line, name), ensure_ascii=False)
            raw_bytes += len(line.encode("utf-8"))
            f.write(s + "\n")
    final = d / name
    tmp.rename(final)
    gz = final.stat().st_size
    log_event("flush", file=str(final), messages=len(batch), bytes_raw=raw_bytes,
              bytes_gz=gz, window_s=secs)
    obs.annotate(messages=len(batch), bytes_raw=raw_bytes, bytes_gz=gz, window_s=secs,
                 file=name, partition=f"dt={window_end:%Y-%m-%d}/hour={window_end:%H}")
    (METRICS_DIR / "heartbeat.json").write_text(json.dumps(
        {"ts": utcnow().isoformat(), "run_id": RUN_ID, "last_file": name, "messages": len(batch)}))


def main():
    if not CLIENT_ID or not CLIENT_SECRET:
        sys.exit("Set BW_CLIENT_ID and BW_CLIENT_SECRET")
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    obs.init("ais-collector", CFG["env"])     # no-op unless OTEL_EXPORTER_OTLP_ENDPOINT is set
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    threading.Thread(target=reader, daemon=True).start()
    log_event("start", flush_seconds=FLUSH_SECONDS)
    window_start = utcnow()
    while not stop.is_set():
        now = time.time()
        sleep = FLUSH_SECONDS - (now % FLUSH_SECONDS)   # align to wall-clock boundary
        if stop.wait(sleep):
            break
        end = utcnow()
        flush(window_start, end)
        window_start = end
    flush(window_start, utcnow())
    log_event("stop")


if __name__ == "__main__":
    main()
