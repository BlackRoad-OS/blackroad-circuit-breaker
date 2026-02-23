"""
BlackRoad Circuit Breaker - Resilient service call wrapper with state management
"""
from __future__ import annotations
import time
import sqlite3
import logging
import functools
import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)
F = TypeVar("F", bound=Callable[..., Any])


class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


@dataclass
class CallRecord:
    breaker_name: str
    timestamp: float
    success: bool
    duration_ms: float
    error: str = ""

    def to_dict(self) -> Dict:
        return {
            "breaker_name": self.breaker_name,
            "timestamp": self.timestamp,
            "success": self.success,
            "duration_ms": self.duration_ms,
            "error": self.error,
        }


class CircuitOpenError(Exception):
    """Raised when a circuit breaker is OPEN."""
    def __init__(self, name: str, reset_after: float):
        self.name = name
        self.reset_after = reset_after
        super().__init__(f"Circuit '{name}' is OPEN. Retry after {reset_after:.1f}s")


@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3

    # Runtime state (not persisted to DB, managed in memory)
    _state: CircuitState = field(default=CircuitState.CLOSED, init=False, repr=False)
    _failure_count: int = field(default=0, init=False, repr=False)
    _success_count: int = field(default=0, init=False, repr=False)
    _last_failure_time: float = field(default=0.0, init=False, repr=False)
    _half_open_calls: int = field(default=0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    @property
    def state(self) -> CircuitState:
        with self._lock:
            if self._state == CircuitState.OPEN:
                if time.monotonic() - self._last_failure_time >= self.timeout:
                    logger.info("Circuit '%s': OPEN -> HALF_OPEN", self.name)
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self._success_count = 0
            return self._state

    def _record_success(self):
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.success_threshold:
                    logger.info("Circuit '%s': HALF_OPEN -> CLOSED", self.name)
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    self._success_count = 0
            elif self._state == CircuitState.CLOSED:
                self._failure_count = max(0, self._failure_count - 1)

    def _record_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.monotonic()
            if self._state == CircuitState.HALF_OPEN or self._failure_count >= self.failure_threshold:
                if self._state != CircuitState.OPEN:
                    logger.warning("Circuit '%s': -> OPEN (failures=%d)", self.name, self._failure_count)
                self._state = CircuitState.OPEN
                self._success_count = 0

    def allow_request(self) -> bool:
        s = self.state
        if s == CircuitState.CLOSED:
            return True
        if s == CircuitState.OPEN:
            return False
        # HALF_OPEN
        with self._lock:
            if self._half_open_calls < self.half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False

    def reset(self):
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = 0.0
            self._half_open_calls = 0
        logger.info("Circuit '%s' manually RESET", self.name)

    def force_open(self):
        with self._lock:
            self._state = CircuitState.OPEN
            self._last_failure_time = time.monotonic()
        logger.warning("Circuit '%s' manually FORCED OPEN", self.name)

    def time_until_retry(self) -> float:
        if self._state != CircuitState.OPEN:
            return 0.0
        elapsed = time.monotonic() - self._last_failure_time
        return max(0.0, self.timeout - elapsed)


class CircuitBreakerRegistry:
    """Registry of named circuit breakers with SQLite call history."""

    def __init__(self, db_path: str = ":memory:"):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.Lock()
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self._init_db()

    def _init_db(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS call_records (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                breaker_name TEXT NOT NULL,
                timestamp    REAL NOT NULL,
                success      INTEGER NOT NULL,
                duration_ms  REAL NOT NULL,
                error        TEXT,
                recorded_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_breaker_ts ON call_records(breaker_name, timestamp);
            CREATE TABLE IF NOT EXISTS breaker_config (
                name               TEXT PRIMARY KEY,
                failure_threshold  INTEGER,
                success_threshold  INTEGER,
                timeout_sec        REAL,
                half_open_max      INTEGER,
                created_at         TEXT
            );
        """)
        self.conn.commit()

    def get_or_create(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 60.0,
        half_open_max_calls: int = 3,
    ) -> CircuitBreaker:
        with self._lock:
            if name not in self._breakers:
                cb = CircuitBreaker(
                    name=name,
                    failure_threshold=failure_threshold,
                    success_threshold=success_threshold,
                    timeout=timeout,
                    half_open_max_calls=half_open_max_calls,
                )
                self._breakers[name] = cb
                self.conn.execute(
                    "INSERT OR IGNORE INTO breaker_config (name, failure_threshold, success_threshold, timeout_sec, half_open_max, created_at) VALUES (?,?,?,?,?,?)",
                    (name, failure_threshold, success_threshold, timeout, half_open_max_calls, datetime.utcnow().isoformat()),
                )
                self.conn.commit()
            return self._breakers[name]

    def call(
        self,
        name: str,
        fn: Callable,
        *args,
        failure_threshold: int = 5,
        success_threshold: int = 2,
        timeout: float = 60.0,
        **kwargs,
    ) -> Any:
        cb = self.get_or_create(name, failure_threshold=failure_threshold,
                                success_threshold=success_threshold, timeout=timeout)
        if not cb.allow_request():
            raise CircuitOpenError(name, cb.time_until_retry())

        start = time.monotonic()
        error_msg = ""
        success = False
        try:
            result = fn(*args, **kwargs)
            success = True
            cb._record_success()
            return result
        except CircuitOpenError:
            raise
        except Exception as exc:
            error_msg = str(exc)
            cb._record_failure()
            raise
        finally:
            duration_ms = (time.monotonic() - start) * 1000
            record = CallRecord(
                breaker_name=name,
                timestamp=time.time(),
                success=success,
                duration_ms=duration_ms,
                error=error_msg,
            )
            self._persist_record(record)

    def _persist_record(self, record: CallRecord):
        self.conn.execute(
            "INSERT INTO call_records (breaker_name, timestamp, success, duration_ms, error, recorded_at) VALUES (?,?,?,?,?,?)",
            (record.breaker_name, record.timestamp, 1 if record.success else 0, record.duration_ms, record.error, datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def get_state(self, name: str) -> Optional[CircuitState]:
        cb = self._breakers.get(name)
        return cb.state if cb else None

    def reset(self, name: str):
        cb = self._breakers.get(name)
        if cb:
            cb.reset()

    def force_open(self, name: str):
        cb = self.get_or_create(name)
        cb.force_open()

    def stats(self, name: str, window_minutes: float = 5.0) -> Dict:
        cutoff = time.time() - window_minutes * 60
        rows = self.conn.execute(
            "SELECT success, duration_ms, error FROM call_records WHERE breaker_name=? AND timestamp>=?",
            (name, cutoff),
        ).fetchall()
        if not rows:
            return {"calls": 0, "failures": 0, "success_rate": 1.0, "avg_latency_ms": 0.0}
        calls = len(rows)
        failures = sum(1 for r in rows if not r[0])
        avg_latency = sum(r[1] for r in rows) / calls
        return {
            "calls": calls,
            "failures": failures,
            "success_rate": round((calls - failures) / calls, 4),
            "avg_latency_ms": round(avg_latency, 2),
        }

    def health_summary(self) -> Dict[str, Dict]:
        return {
            name: {
                "state": cb.state.value,
                "failure_count": cb._failure_count,
                "stats": self.stats(name),
            }
            for name, cb in self._breakers.items()
        }

    def cleanup_old_records(self, older_than_seconds: float = 3600):
        cutoff = time.time() - older_than_seconds
        cur = self.conn.execute("DELETE FROM call_records WHERE timestamp<?", (cutoff,))
        self.conn.commit()
        return cur.rowcount

    def decorator(
        self,
        name: str,
        failure_threshold: int = 5,
        timeout: float = 60.0,
    ):
        """Decorator factory: @registry.decorator('my_service')"""
        def decorate(fn: F) -> F:
            @functools.wraps(fn)
            def wrapper(*args, **kwargs):
                return self.call(name, fn, *args, failure_threshold=failure_threshold, timeout=timeout, **kwargs)
            return wrapper  # type: ignore
        return decorate


# Module-level default registry
_default_registry = CircuitBreakerRegistry()


def circuit_breaker(
    name: str,
    failure_threshold: int = 5,
    timeout: float = 60.0,
):
    """Module-level decorator: @circuit_breaker('my_service')"""
    return _default_registry.decorator(name, failure_threshold=failure_threshold, timeout=timeout)


def get_default_registry() -> CircuitBreakerRegistry:
    return _default_registry
