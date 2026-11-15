"""Replicable deployment: the systemd templates render for any user and folder, and Jaeger stays on localhost."""
import os, subprocess
from pathlib import Path

ROOT = Path(__file__).parent.parent


def test_units_render_for_any_machine(tmp_path):
    env = dict(os.environ, REPO="/srv/ais", SVC_USER="alice", VENV="/opt/venv", JAVA_HOME="/opt/jdk", JAEGER_DIR="/opt/jaeger")
    subprocess.run(["bash", str(ROOT / "scripts" / "install_services.sh"), "--render", str(tmp_path)], check=True, env=env)
    units = {p.name: p.read_text() for p in tmp_path.iterdir()}
    templates = {p.name[len("deploy-"):] for p in ROOT.glob("deploy-*") if p.suffix in (".service", ".timer")}
    assert set(units) == templates and "ais-collector.service" in units and "jaeger.service" in units
    for name, text in units.items():
        assert "@" not in text.replace("multi-user.target", ""), name          # every placeholder filled
        assert "/home/ubuntu" not in text and "User=ubuntu" not in text, name
    c = units["ais-collector.service"]
    assert "User=alice" in c and "WorkingDirectory=/srv/ais" in c and "ExecStart=/opt/venv/bin/python" in c
    assert "JAVA_HOME=/opt/jdk" in c
    assert "EnvironmentFile=/opt/jaeger/jaeger.env" in units["jaeger.service"]


def test_jaeger_config_binds_localhost_only():
    endpoints = [l.split("endpoint:")[1].strip() for l in (ROOT / "config" / "jaeger.yaml").read_text().splitlines()
                 if "endpoint:" in l]
    assert len(endpoints) == 4 and all(e.startswith("localhost:") for e in endpoints)


def test_env_example_lists_every_setting():
    text = (ROOT / ".env.example").read_text()
    for key in ("APP_ENV", "BW_CLIENT_ID", "BW_CLIENT_SECRET", "GFW_TOKEN", "OTEL_EXPORTER_OTLP_ENDPOINT"):
        assert key + "=" in text, key
