import pytest
import pipeline_config
from collector.collector import make_record
from spark.silver_logic import BRONZE_SCHEMA, PAYLOAD_SCHEMA


@pytest.mark.parametrize("env", ["dev", "prod"])
def test_config_complete_and_separated(env):
    cfg = pipeline_config.load(env)
    assert cfg["env"] == env


def test_dev_and_prod_do_not_share_paths():
    d, p = pipeline_config.load("dev"), pipeline_config.load("prod")
    assert d["data_root"] != p["data_root"] and d["metrics_dir"] != p["metrics_dir"]


def test_bronze_record_contract_matches_silver_schema():
    rec = make_record("2026-01-01T00:00:00+00:00", '{"mmsi":1}', "f.jsonl.gz", "run1")
    assert set(rec) == {f.name for f in BRONZE_SCHEMA.fields}
    assert rec["payload"] == '{"mmsi":1}'                      # raw payload untouched


def test_essential_fields_in_payload_schema():
    names = {f.name for f in PAYLOAD_SCHEMA.fields}
    assert {"mmsi", "msgtime", "latitude", "longitude"} <= names
