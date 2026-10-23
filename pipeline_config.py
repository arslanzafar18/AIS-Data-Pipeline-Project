"""Environment-specific settings. Select with APP_ENV=dev|prod (default: dev). Stdlib only."""
import json, os
from pathlib import Path

REQUIRED = ["env", "data_root", "metrics_dir", "flush_seconds", "spark_master", "shuffle_partitions",
            "near_m", "at_m", "stationary_kn", "visit_gap_s", "cross_batch_dedupe", "lookback_days", "gate"]
GATE_REQUIRED = ["min_silver_rows", "max_reject_rate", "max_silver_staleness_min"]


def load(env=None):
    env = env or os.environ.get("APP_ENV", "dev")
    cfg = json.loads((Path(__file__).parent / "config" / f"{env}.json").read_text())
    missing = [k for k in REQUIRED if k not in cfg] + [f"gate.{k}" for k in GATE_REQUIRED if k not in cfg.get("gate", {})]
    if missing:
        raise ValueError(f"config {env}: missing keys {missing}")
    if cfg["env"] != env:
        raise ValueError(f"config {env}: 'env' field says {cfg['env']}")
    return cfg


def path(cfg, rel):
    return str(Path(cfg["data_root"]) / rel)
