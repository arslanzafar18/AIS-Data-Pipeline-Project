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

MAX_BACKOFF_SECONDS = 60        # a live stream is not replayed: every second disconnected is data lost
AUTH_RETRY_SECONDS = 600        # rejected credentials: retrying faster cannot help, a person must fix .env

RUN_ID = uuid.uuid4().hex[:12]
lock = threading.Lock()
buffer: list = []          # (ingest_ts_iso, raw_line)
stop = threading.Event()


def utcnow():
    return datetime.now(timezone.utc)


def get_token():
    """OAuth2 client-credentials token, and the time to renew it (2 minutes before it expires)."""
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


def is_auth_failure(error):
    """True if the token server or the stream refused our credentials: HTTP 401 or 403, or 400 from the token
    server (OAuth2 servers answer a wrong client secret with 400 invalid_client)."""
    resp = getattr(error, "response", None)
    status = getattr(resp, "status_code", None)
    return status in (401, 403) or (status == 400 and str(getattr(resp, "url", "")).startswith(TOKEN_URL))


def retry_wait(error, backoff):
    """(seconds to wait now, backoff for the next failure). Any failure doubles the wait, 1, 2, 4 ... up to
    MAX_BACKOFF_SECONDS; rejected credentials wait AUTH_RETRY_SECONDS instead, because only a person can fix
    them (the monitor alerts after 10 minutes without a new Bronze file)."""
    if is_auth_failure(error):
        return AUTH_RETRY_SECONDS, backoff
    return backoff, min(backoff * 2, MAX_BACKOFF_SECONDS)


def read_stream(resp, expiry):
    """Append every line of one open stream to the buffer until it closes, we stop, or the token is about to
    expire. Returns (why it ended, messages read)."""
    n = 0
    with resp:
        for line in resp.iter_lines():
            if stop.is_set():
                return "stopping", n
            if time.time() > expiry:
                return "token_refresh", n       # reader() reconnects with a fresh token
            if line:
                n += 1
                with lock:
                    buffer.append((utcnow().isoformat(), line.decode("utf-8", "replace")))
    return "stream_closed", n


def reader():
    """Runs in its own thread: connect, read the stream, and reconnect after any failure with a growing wait."""
    backoff, attempt = 1, 0
    while not stop.is_set():
        attempt += 1
        try:
            resp, expiry = connect(attempt)
            backoff, started = 1, time.time()
            reason, n = read_stream(resp, expiry)
            # A stream lasts up to an hour, so it is not one long span: a short span records how it ended.
            with obs.span("bronze.stream_end", reason=reason, messages=n,
                          duration_s=round(time.time() - started, 1), attempt=attempt):
                pass
        except Exception as e:
            wait, backoff = retry_wait(e, backoff)
            auth = is_auth_failure(e)
            log_event("reader_error", error=repr(e), backoff=wait, auth_failure=auth)
            with obs.span("bronze.reader_error", error=repr(e)[:300], backoff_s=wait, attempt=attempt,
                          auth_failure=auth):
                obs.mark_error(type(e).__name__)
            stop.wait(wait)             # unlike time.sleep, a shutdown does not have to wait out a long pause


def make_record(ingest_ts, line, file_name, run_id=None):
    """Bronze record contract: raw payload untouched + lineage metadata."""
    return {"_source": SOURCE, "_ingest_ts": ingest_ts, "_run_id": run_id or RUN_ID,
            "_file": file_name, "payload": line}


def log_event(kind, **kw):
    """One line in metrics/events.jsonl (start, flush, reader_error, stop); the health tab reads it."""
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    rec = {"ts": utcnow().isoformat(), "run_id": RUN_ID, "event": kind, **kw}
    with open(METRICS_DIR / "events.jsonl", "a") as f:
        f.write(json.dumps(rec) + "\n")


def write_bronze_file(batch, window_end):
    """Write one gzip JSONL file into bronze/raw/dt=YYYY-MM-DD/hour=HH/. It is written under a dot-name first
    (Spark ignores dot-files) and renamed when complete, so Silver never reads half a file.
    Returns (final path, raw bytes before compression)."""
    d = BRONZE_DIR / f"dt={window_end:%Y-%m-%d}" / f"hour={window_end:%H}"
    d.mkdir(parents=True, exist_ok=True)
    name = f"ais_{RUN_ID}_{window_end:%Y%m%dT%H%M%SZ}.jsonl.gz"
    tmp = d / f".tmp_{name}"
    raw_bytes = 0
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        for ts, line in batch:
            raw_bytes += len(line.encode("utf-8"))
            f.write(json.dumps(make_record(ts, line, name), ensure_ascii=False) + "\n")
    final = d / name
    tmp.rename(final)
    return final, raw_bytes


def flush(window_start, window_end):
    """Take everything buffered since the last flush and write it to Bronze (traced as one span)."""
    with obs.span("bronze.flush", source=SOURCE, run_id=RUN_ID):
        _flush(window_start, window_end)


def _flush(window_start, window_end):
    with lock:
        batch = buffer[:]
        buffer.clear()
    secs = (window_end - window_start).total_seconds()
    if not batch:                       # logged too, so a quiet stream is visible on the health tab
        log_event("flush", file=None, messages=0, bytes_raw=0, bytes_gz=0, window_s=secs)
        obs.annotate(messages=0, window_s=secs)
        obs.event("empty_window")
        return
    final, raw_bytes = write_bronze_file(batch, window_end)
    gz = final.stat().st_size
    log_event("flush", file=str(final), messages=len(batch), bytes_raw=raw_bytes, bytes_gz=gz, window_s=secs)
    obs.annotate(messages=len(batch), bytes_raw=raw_bytes, bytes_gz=gz, window_s=secs,
                 file=final.name, partition=f"dt={window_end:%Y-%m-%d}/hour={window_end:%H}")
    (METRICS_DIR / "heartbeat.json").write_text(json.dumps(       # read by the monitor's stale-heartbeat alert
        {"ts": utcnow().isoformat(), "run_id": RUN_ID, "last_file": final.name, "messages": len(batch)}))


def main():
    """Start the reader thread, then cut a Bronze file at every FLUSH_SECONDS wall-clock boundary until stopped."""
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
