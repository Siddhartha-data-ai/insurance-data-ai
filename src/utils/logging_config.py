"""
Structured JSON Logging Configuration
Enterprise-grade logging with correlation IDs and context propagation
"""

import json
import logging
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, Optional
from functools import wraps
import uuid


class StructuredLogger:
    """
    Structured JSON logger for insurance data platform
    Provides consistent logging format across all services
    """

    def __init__(self, name: str, level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))
        self.logger.handlers = []  # Clear existing handlers

        # Create console handler with JSON formatter
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        self.logger.addHandler(handler)

        # Context for correlation IDs and metadata
        self.context: Dict[str, Any] = {}

    def set_context(self, **kwargs):
        """Set logging context (e.g., request_id, user_id, session_id)"""
        self.context.update(kwargs)

    def clear_context(self):
        """Clear logging context"""
        self.context = {}

    def _build_log_entry(self, level: str, message: str, **kwargs) -> Dict[str, Any]:
        """Build structured log entry"""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": level,
            "message": message,
            "logger": self.logger.name,
            **self.context,  # Include context (request_id, etc.)
            **kwargs,  # Additional metadata
        }
        return log_entry

    def info(self, message: str, **kwargs):
        """Log info message"""
        self.logger.info(json.dumps(self._build_log_entry("INFO", message, **kwargs)))

    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self.logger.warning(json.dumps(self._build_log_entry("WARNING", message, **kwargs)))

    def error(self, message: str, error: Optional[Exception] = None, **kwargs):
        """Log error message with optional exception details"""
        if error:
            kwargs.update(
                {
                    "error_type": type(error).__name__,
                    "error_message": str(error),
                    "stack_trace": traceback.format_exc(),
                }
            )
        self.logger.error(json.dumps(self._build_log_entry("ERROR", message, **kwargs)))

    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self.logger.debug(json.dumps(self._build_log_entry("DEBUG", message, **kwargs)))

    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self.logger.critical(json.dumps(self._build_log_entry("CRITICAL", message, **kwargs)))


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging"""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        # If message is already JSON, return as-is
        try:
            json.loads(record.getMessage())
            return record.getMessage()
        except (json.JSONDecodeError, ValueError):
            # Otherwise, create JSON structure
            log_entry = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "level": record.levelname,
                "message": record.getMessage(),
                "logger": record.name,
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
            }

            if record.exc_info:
                log_entry["exception"] = self.formatException(record.exc_info)

            return json.dumps(log_entry)


# Global logger instance
_loggers: Dict[str, StructuredLogger] = {}


def get_logger(name: str, level: str = "INFO") -> StructuredLogger:
    """
    Get or create a structured logger instance

    Args:
        name: Logger name (typically __name__)
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

    Returns:
        StructuredLogger instance
    """
    if name not in _loggers:
        _loggers[name] = StructuredLogger(name, level)
    return _loggers[name]


def log_execution_time(logger: Optional[StructuredLogger] = None):
    """
    Decorator to log function execution time

    Usage:
        @log_execution_time(logger)
        def my_function():
            pass
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _logger = logger or get_logger(func.__module__)
            start_time = datetime.utcnow()
            function_name = func.__name__

            _logger.info(f"Starting execution: {function_name}", function=function_name, event="function_start")

            try:
                result = func(*args, **kwargs)
                execution_time = (datetime.utcnow() - start_time).total_seconds()

                _logger.info(
                    f"Completed execution: {function_name}",
                    function=function_name,
                    execution_time_seconds=execution_time,
                    event="function_complete",
                    status="success",
                )

                return result

            except Exception as e:
                execution_time = (datetime.utcnow() - start_time).total_seconds()

                _logger.error(
                    f"Failed execution: {function_name}",
                    error=e,
                    function=function_name,
                    execution_time_seconds=execution_time,
                    event="function_failed",
                    status="error",
                )
                raise

        return wrapper

    return decorator


def generate_correlation_id() -> str:
    """Generate a unique correlation ID for request tracking"""
    return str(uuid.uuid4())


# Example usage
if __name__ == "__main__":
    # Create logger
    logger = get_logger("insurance.etl", level="INFO")

    # Set context for correlation
    logger.set_context(request_id="req-12345", user_id="user-001", environment="production")

    # Log messages
    logger.info("Starting ETL pipeline", pipeline="bronze_to_silver", table="customers")

    logger.warning("Data quality issue detected", table="customers_raw", issue="null_values", count=15)

    try:
        # Simulate error
        raise ValueError("Invalid data format")
    except Exception as e:
        logger.error("ETL pipeline failed", error=e, pipeline="bronze_to_silver")

    # Use decorator
    @log_execution_time(logger)
    def process_data():
        """Sample function"""
        import time

        time.sleep(1)
        return "done"

    process_data()
