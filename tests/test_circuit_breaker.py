"""Tests for BlackRoad Circuit Breaker"""
import time
import pytest
from circuit_breaker import (
    CircuitBreaker, CircuitBreakerRegistry, CircuitState,
    CircuitOpenError, CallRecord, circuit_breaker, get_default_registry,
)


def always_succeed():
    return "ok"


def always_fail():
    raise ValueError("service down")


def slow_call():
    time.sleep(0.05)
    return "done"


@pytest.fixture
def registry():
    return CircuitBreakerRegistry(":memory:")


@pytest.fixture
def cb():
    return CircuitBreaker("test", failure_threshold=3, timeout=1.0)


class TestCircuitBreakerState:
    def test_initial_state_closed(self, cb):
        assert cb.state == CircuitState.CLOSED

    def test_opens_after_threshold(self, cb):
        for _ in range(cb.failure_threshold):
            cb._record_failure()
        assert cb.state == CircuitState.OPEN

    def test_allow_request_when_closed(self, cb):
        assert cb.allow_request() is True

    def test_deny_request_when_open(self, cb):
        cb.force_open()
        assert cb.allow_request() is False

    def test_half_open_after_timeout(self, cb):
        cb.force_open()
        cb._last_failure_time = time.monotonic() - cb.timeout - 0.1
        assert cb.state == CircuitState.HALF_OPEN

    def test_closes_after_success_threshold(self, cb):
        cb.force_open()
        cb._last_failure_time = time.monotonic() - cb.timeout - 0.1
        _ = cb.state  # triggers HALF_OPEN transition
        for _ in range(cb.success_threshold):
            cb._record_success()
        assert cb.state == CircuitState.CLOSED

    def test_reset(self, cb):
        for _ in range(cb.failure_threshold):
            cb._record_failure()
        assert cb.state == CircuitState.OPEN
        cb.reset()
        assert cb.state == CircuitState.CLOSED

    def test_force_open(self, cb):
        cb.force_open()
        assert cb.state == CircuitState.OPEN

    def test_time_until_retry(self, cb):
        cb.force_open()
        t = cb.time_until_retry()
        assert t > 0

    def test_time_until_retry_zero_when_closed(self, cb):
        assert cb.time_until_retry() == 0.0


class TestRegistryCall:
    def test_successful_call(self, registry):
        result = registry.call("svc", always_succeed)
        assert result == "ok"

    def test_failed_call_raises(self, registry):
        with pytest.raises(ValueError):
            registry.call("svc", always_fail, failure_threshold=5)

    def test_circuit_opens_on_threshold(self, registry):
        cb = registry.get_or_create("svc2", failure_threshold=2)
        for _ in range(2):
            with pytest.raises(ValueError):
                registry.call("svc2", always_fail)
        assert registry.get_state("svc2") == CircuitState.OPEN

    def test_circuit_open_error_raised(self, registry):
        registry.force_open("svc3")
        with pytest.raises(CircuitOpenError):
            registry.call("svc3", always_succeed)

    def test_stats_after_calls(self, registry):
        registry.call("svc4", always_succeed)
        with pytest.raises(ValueError):
            registry.call("svc4", always_fail, failure_threshold=10)
        stats = registry.stats("svc4")
        assert stats["calls"] == 2
        assert stats["failures"] == 1
        assert stats["success_rate"] == 0.5

    def test_stats_empty(self, registry):
        stats = registry.stats("unknown_svc")
        assert stats["calls"] == 0
        assert stats["success_rate"] == 1.0

    def test_health_summary(self, registry):
        registry.get_or_create("svcA")
        registry.get_or_create("svcB")
        summary = registry.health_summary()
        assert "svcA" in summary
        assert "svcB" in summary
        assert summary["svcA"]["state"] == "CLOSED"

    def test_slow_call_records_latency(self, registry):
        registry.call("slow_svc", slow_call)
        stats = registry.stats("slow_svc")
        assert stats["avg_latency_ms"] >= 40  # at least 40ms

    def test_cleanup_old_records(self, registry):
        registry.call("svc5", always_succeed)
        deleted = registry.cleanup_old_records(older_than_seconds=-1)
        assert deleted >= 1

    def test_reset_via_registry(self, registry):
        registry.force_open("svc6")
        registry.reset("svc6")
        assert registry.get_state("svc6") == CircuitState.CLOSED


class TestDecorator:
    def test_decorator(self, registry):
        @registry.decorator("decorated_svc")
        def my_func():
            return "decorated"
        result = my_func()
        assert result == "decorated"

    def test_decorator_raises_on_open(self, registry):
        @registry.decorator("open_svc")
        def failing():
            raise RuntimeError("down")
        registry.force_open("open_svc")
        with pytest.raises(CircuitOpenError):
            failing()


class TestCallRecord:
    def test_to_dict(self):
        rec = CallRecord("svc", time.time(), True, 12.5)
        d = rec.to_dict()
        assert d["breaker_name"] == "svc"
        assert d["success"] is True
