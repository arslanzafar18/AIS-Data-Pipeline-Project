"""The collector sends a span per Bronze file with message and byte counts."""
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

import observability as obs
from collector import collector as C


class _Tracing:
    """Private provider, so this test does not touch the global one other tests set."""
    def __init__(self, provider):
        self.p = provider

    def get_tracer(self, name):
        return self.p.get_tracer(name)

    def get_current_span(self):
        return trace.get_current_span()


def _spans(monkeypatch):
    exp = InMemorySpanExporter()
    p = TracerProvider()
    p.add_span_processor(SimpleSpanProcessor(exp))
    monkeypatch.setattr(obs, "trace", _Tracing(p))
    return exp


def test_flush_span_counts_messages_and_bytes(monkeypatch, tmp_path):
    exp = _spans(monkeypatch)
    monkeypatch.setattr(C, "BRONZE_DIR", tmp_path / "bronze")
    monkeypatch.setattr(C, "METRICS_DIR", tmp_path / "metrics")
    C.buffer[:] = [("2026-11-14T10:00:00+00:00", '{"mmsi": 1}'), ("2026-11-14T10:00:01+00:00", '{"mmsi": 2}')]
    start, end = C.utcnow(), C.utcnow()
    C.flush(start, end)
    C.flush(start, end)                     # buffer now empty
    a, b = [s for s in exp.get_finished_spans() if s.name == "bronze.flush"]
    assert a.attributes["messages"] == 2 and a.attributes["bytes_gz"] > 0
    assert a.attributes["partition"].startswith(f"dt={end:%Y-%m-%d}/hour=")
    assert b.attributes["messages"] == 0 and b.events[0].name == "empty_window"
    assert len(list((tmp_path / "bronze").rglob("*.jsonl.gz"))) == 1


def test_failed_connect_is_an_error_span(monkeypatch):
    exp = _spans(monkeypatch)

    def no_token():
        raise ConnectionError("token endpoint down")
    monkeypatch.setattr(C, "get_token", no_token)
    try:
        C.connect(1)
    except ConnectionError:
        pass
    (s,) = [s for s in exp.get_finished_spans() if s.name == "bronze.connect"]
    assert s.status.status_code.name == "ERROR" and s.attributes["attempt"] == 1
