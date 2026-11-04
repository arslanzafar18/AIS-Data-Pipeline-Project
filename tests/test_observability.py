import observability as obs


def test_tracing_is_noop_without_config(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_TRACES_EXPORTER", raising=False)
    obs.init("test", "dev")
    with obs.span("a", x=1):
        obs.annotate(k=1, skipped=None)
        obs.event("e", n=2)
        obs.mark_error("boom")
    obs.start("b").end()


def test_spans_nest_and_carry_attributes():
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
    exp = InMemorySpanExporter()
    p = TracerProvider()
    p.add_span_processor(SimpleSpanProcessor(exp))
    trace.set_tracer_provider(p)
    with obs.span("outer", batch_id=3):
        obs.annotate(rows=10, failed=["a", "b"])
        obs.start("inner").end()
        obs.mark_error("gate failed")
    spans = {s.name: s for s in exp.get_finished_spans()}
    assert set(spans) == {"outer", "inner"}
    assert spans["inner"].parent.span_id == spans["outer"].context.span_id
    assert spans["outer"].attributes["rows"] == 10 and spans["outer"].attributes["failed"] == "a,b"
    assert spans["outer"].status.status_code.name == "ERROR"
