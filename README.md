# blackroad-circuit-breaker

> Circuit breaker pattern for resilient service calls — part of the BlackRoad OS developer platform.

## Features

- 🔴 **Three States** — CLOSED, OPEN, HALF_OPEN with automatic transitions
- ⚡ **Async-Safe** — Thread-safe with proper locking
- 📊 **Stats** — Per-breaker call statistics with windowed queries
- 💾 **History** — SQLite call history with cleanup
- 🎯 **Decorator API** — `@circuit_breaker('service_name')` syntax
- 🏥 **Health Summary** — All breakers at a glance
- 🔧 **Manual Control** — `force_open()`, `reset()`

## Quick Start

```python
from circuit_breaker import CircuitBreakerRegistry, circuit_breaker

registry = CircuitBreakerRegistry()

# Wrap a function call
result = registry.call("payment_service", call_payment_api, amount=100)

# Decorator API
@registry.decorator("email_service", failure_threshold=3, timeout=30.0)
def send_email(to, body):
    return external_email_api(to, body)

# Module-level decorator
@circuit_breaker("inventory_service")
def check_inventory(item_id):
    return inventory_api(item_id)

# Health
print(registry.health_summary())
```

## Running Tests

```bash
pip install pytest pytest-cov
pytest tests/ -v --cov=circuit_breaker
```

## License

Proprietary — © BlackRoad OS, Inc.
