"""OpenTelemetry tracing for the pipeline (Silver and Gold; collector later).

Off by default: unless OTEL_EXPORTER_OTLP_ENDPOINT or OTEL_TRACES_EXPORTER is set, every span is a no-op,
so tests, CI and the demo need nothing. Examples:
  OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318   -> Jaeger (or any OTLP backend)
  OTEL_TRACES_EXPORTER=console                       -> print spans as JSON
If the opentelemetry packages are missing, everything here is a stub: tracing never takes the pipeline down.
Spark is lazy: wrap ACTIONS (collect, count, write) in spans, not transformations.
"""
import os
from contextlib import nullcontext

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
except ImportError:
    trace = None

NAME = "ais-pipeline"


class _NoSpan:
    def end(self):
        pass


def init(service_name, env="unknown"):
    if trace is None:
        return
    mode = os.environ.get("OTEL_TRACES_EXPORTER") or ("otlp" if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") else "none")
    if mode == "none":
        return
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    if mode == "console":
        exporter = ConsoleSpanExporter()
    else:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        exporter = OTLPSpanExporter()            # reads OTEL_EXPORTER_OTLP_ENDPOINT
    provider = TracerProvider(resource=Resource.create({"service.name": service_name, "deployment.environment": env}))
    provider.add_span_processor(BatchSpanProcessor(exporter))   # flushed automatically at process exit
    trace.set_tracer_provider(provider)


def _clean(d):
    out = {}
    for k, v in (d or {}).items():
        if isinstance(v, (list, tuple)):
            v = ",".join(map(str, v))
        if isinstance(v, (str, bool, int, float)):
            out[k] = v
    return out


def span(name, /, **attrs):
    """Context manager: a child of the current span (or a new trace)."""
    if trace is None:
        return nullcontext()
    return trace.get_tracer(NAME).start_as_current_span(name, attributes=_clean(attrs))


def start(name, /, **attrs):
    """Manual span (call .end()) for code that does not fit a with-block."""
    if trace is None:
        return _NoSpan()
    return trace.get_tracer(NAME).start_span(name, attributes=_clean(attrs))


def annotate(**attrs):
    if trace is not None:
        trace.get_current_span().set_attributes(_clean(attrs))


def event(name, /, **attrs):
    if trace is not None:
        trace.get_current_span().add_event(name, _clean(attrs))


def mark_error(msg):
    if trace is not None:
        trace.get_current_span().set_status(Status(StatusCode.ERROR, msg))
