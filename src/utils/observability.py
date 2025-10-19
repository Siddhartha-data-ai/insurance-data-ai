"""
OpenTelemetry Distributed Tracing
End-to-end request tracing for insurance data platform
"""

import json
import time
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, Optional


class TracingContext:
    """
    Tracing context for distributed tracing
    Simulates OpenTelemetry tracing (simplified version)
    """

    def __init__(self):
        self.traces: Dict[str, Dict[str, Any]] = {}
        self.active_spans: Dict[str, str] = {}

    def create_trace(self, trace_id: str, operation_name: str) -> str:
        """Create a new trace"""
        self.traces[trace_id] = {
            "trace_id": trace_id,
            "operation_name": operation_name,
            "start_time": datetime.utcnow().isoformat(),
            "spans": [],
            "status": "in_progress",
        }
        return trace_id

    def start_span(self, trace_id: str, span_name: str, span_type: str = "internal") -> str:
        """Start a new span within a trace"""
        if trace_id not in self.traces:
            self.create_trace(trace_id, "unknown")

        span_id = f"{trace_id}-span-{len(self.traces[trace_id]['spans']) + 1}"

        span = {
            "span_id": span_id,
            "span_name": span_name,
            "span_type": span_type,  # internal, database, http, ml_model
            "start_time": datetime.utcnow().isoformat(),
            "end_time": None,
            "duration_ms": None,
            "attributes": {},
            "events": [],
            "status": "in_progress",
        }

        self.traces[trace_id]["spans"].append(span)
        self.active_spans[span_id] = trace_id

        return span_id

    def add_span_attribute(self, span_id: str, key: str, value: Any):
        """Add attribute to span"""
        trace_id = self.active_spans.get(span_id)
        if not trace_id:
            return

        for span in self.traces[trace_id]["spans"]:
            if span["span_id"] == span_id:
                span["attributes"][key] = value
                break

    def add_span_event(self, span_id: str, event_name: str, **attributes):
        """Add event to span"""
        trace_id = self.active_spans.get(span_id)
        if not trace_id:
            return

        for span in self.traces[trace_id]["spans"]:
            if span["span_id"] == span_id:
                span["events"].append(
                    {
                        "event_name": event_name,
                        "timestamp": datetime.utcnow().isoformat(),
                        "attributes": attributes,
                    }
                )
                break

    def end_span(self, span_id: str, status: str = "success"):
        """End a span"""
        trace_id = self.active_spans.get(span_id)
        if not trace_id:
            return

        for span in self.traces[trace_id]["spans"]:
            if span["span_id"] == span_id:
                span["end_time"] = datetime.utcnow().isoformat()
                span["status"] = status

                # Calculate duration
                start = datetime.fromisoformat(span["start_time"].replace("Z", ""))
                end = datetime.fromisoformat(span["end_time"].replace("Z", ""))
                span["duration_ms"] = int((end - start).total_seconds() * 1000)

                break

        del self.active_spans[span_id]

    def end_trace(self, trace_id: str, status: str = "success"):
        """End a trace"""
        if trace_id in self.traces:
            self.traces[trace_id]["end_time"] = datetime.utcnow().isoformat()
            self.traces[trace_id]["status"] = status

            # Calculate total duration
            start = datetime.fromisoformat(self.traces[trace_id]["start_time"].replace("Z", ""))
            end = datetime.fromisoformat(self.traces[trace_id]["end_time"].replace("Z", ""))
            self.traces[trace_id]["duration_ms"] = int((end - start).total_seconds() * 1000)

    def get_trace(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """Get trace details"""
        return self.traces.get(trace_id)

    def export_trace(self, trace_id: str) -> str:
        """Export trace as JSON"""
        trace = self.get_trace(trace_id)
        return json.dumps(trace, indent=2) if trace else "{}"


# Global tracing context
_tracing_context = TracingContext()


def get_tracer() -> TracingContext:
    """Get global tracing context"""
    return _tracing_context


def trace_operation(operation_name: str, span_type: str = "internal"):
    """
    Decorator to trace function execution

    Args:
        operation_name: Name of the operation
        span_type: Type of span (internal, database, http, ml_model)

    Usage:
        @trace_operation("process_claims", span_type="internal")
        def process_claims():
            pass
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracer()

            # Generate IDs
            trace_id = f"trace-{int(time.time() * 1000)}"
            span_name = f"{func.__module__}.{func.__name__}"

            # Create trace and span
            tracer.create_trace(trace_id, operation_name)
            span_id = tracer.start_span(trace_id, span_name, span_type)

            # Add function metadata
            tracer.add_span_attribute(span_id, "function", func.__name__)
            tracer.add_span_attribute(span_id, "module", func.__module__)

            try:
                # Execute function
                result = func(*args, **kwargs)

                # Mark as successful
                tracer.end_span(span_id, status="success")
                tracer.end_trace(trace_id, status="success")

                return result

            except Exception as e:
                # Mark as failed
                tracer.add_span_attribute(span_id, "error", str(e))
                tracer.add_span_attribute(span_id, "error_type", type(e).__name__)
                tracer.add_span_event(span_id, "exception", error=str(e))

                tracer.end_span(span_id, status="error")
                tracer.end_trace(trace_id, status="error")

                raise

        return wrapper

    return decorator


class PerformanceMonitor:
    """Monitor and log performance metrics"""

    def __init__(self):
        self.metrics: Dict[str, list] = {}

    def record_metric(self, metric_name: str, value: float, unit: str = "ms", **labels: Dict[str, str]):
        """
        Record a performance metric

        Args:
            metric_name: Name of the metric (e.g., "query_duration", "rows_processed")
            value: Metric value
            unit: Unit of measurement (ms, seconds, count, bytes)
            labels: Additional labels (e.g., table="customers", operation="read")
        """
        if metric_name not in self.metrics:
            self.metrics[metric_name] = []

        self.metrics[metric_name].append(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "value": value,
                "unit": unit,
                "labels": labels,
            }
        )

    def get_metric_summary(self, metric_name: str) -> Dict[str, Any]:
        """Get summary statistics for a metric"""
        if metric_name not in self.metrics or not self.metrics[metric_name]:
            return {}

        values = [m["value"] for m in self.metrics[metric_name]]

        return {
            "metric_name": metric_name,
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "total": sum(values),
            "unit": self.metrics[metric_name][0]["unit"],
        }

    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get summary of all metrics"""
        return {metric_name: self.get_metric_summary(metric_name) for metric_name in self.metrics}


# Global performance monitor
_performance_monitor = PerformanceMonitor()


def get_performance_monitor() -> PerformanceMonitor:
    """Get global performance monitor"""
    return _performance_monitor


# Example usage
if __name__ == "__main__":
    # Tracing example
    @trace_operation("ETL Pipeline", span_type="internal")
    def run_etl_pipeline():
        """Sample ETL pipeline"""
        tracer = get_tracer()
        trace_id = list(tracer.traces.keys())[-1]

        # Simulate database read
        span_id = tracer.start_span(trace_id, "read_source_data", "database")
        tracer.add_span_attribute(span_id, "table", "customers_raw")
        tracer.add_span_attribute(span_id, "rows_read", 10000)
        time.sleep(0.1)  # Simulate work
        tracer.end_span(span_id)

        # Simulate transformation
        span_id = tracer.start_span(trace_id, "transform_data", "internal")
        tracer.add_span_attribute(span_id, "transformation", "data_quality_checks")
        time.sleep(0.05)
        tracer.end_span(span_id)

        # Simulate database write
        span_id = tracer.start_span(trace_id, "write_target_data", "database")
        tracer.add_span_attribute(span_id, "table", "customers_silver")
        tracer.add_span_attribute(span_id, "rows_written", 9995)
        time.sleep(0.1)
        tracer.end_span(span_id)

        return "success"

    # Execute traced function
    result = run_etl_pipeline()

    # Get and print trace
    tracer = get_tracer()
    traces = list(tracer.traces.keys())
    if traces:
        print("\n=== Distributed Trace ===")
        print(tracer.export_trace(traces[-1]))

    # Performance monitoring example
    monitor = get_performance_monitor()

    monitor.record_metric("query_duration", 150.5, unit="ms", table="customers", operation="read")
    monitor.record_metric("query_duration", 200.3, unit="ms", table="policies", operation="read")
    monitor.record_metric("rows_processed", 10000, unit="count", pipeline="bronze_to_silver")

    print("\n=== Performance Metrics ===")
    print(json.dumps(monitor.get_all_metrics(), indent=2))
