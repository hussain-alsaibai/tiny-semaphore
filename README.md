# tiny-semaphore — Zero-Dependency Concurrency Limiter for Python

> **"Don't drown your database. Limit your parallelism."**
> Async-aware, fair FIFO semaphore. ~80 lines. No dependencies.

## Why a Semaphore?

Every system has a concurrency limit. Your database might handle 20 connections, your API might rate-limit at 100 req/s, your LLM provider might cap at 10 parallel calls.

A semaphore limits how many operations can run simultaneously — and it works correctly with `async/await`, unlike `threading.Semaphore`.

## Installation

```bash
pip install tiny-semaphore
```

Or copy `semaphore.py` into your project. Zero dependencies.

## Quick Start

```python
from tiny_semaphore import Semaphore

# Limit to 5 concurrent database writes
db_sem = Semaphore(max_permits=5, name="db-writes")

async def write_row(data):
    async with db_sem:
        await db.insert(data)

# Run 1000 writes, but only 5 at a time
results = await asyncio.gather(*[write_row(d) for d in all_data])
```

## Why Not `asyncio.Semaphore`?

Python's built-in `asyncio.Semaphore` is great — but it's *fairness-blind*. If you acquire it 100 times and release once, the other 99 wait indefinitely.

`tiny-semaphore` adds:
- **Timeout on acquire** — don't wait forever
- **Fair FIFO queue** — first-waiting task goes first
- **Stats** — how many waiters, current permits, peak usage
- **Context manager + manual** — acquire/release or use `async with`
- **Batch acquire** — grab multiple permits at once

## API Reference

```python
from tiny_semaphore import Semaphore

sem = Semaphore(
    max_permits=10,    # Maximum concurrent holders
    name="api-calls",  # Label for observability
)

# Async context manager (recommended)
async with sem:
    await do_work()

# Manual acquire / release
await sem.acquire()
try:
    await do_work()
finally:
    sem.release()

# Acquire multiple permits at once
async with sem.acquire_many(3):
    await do_heavy_work()

# Try without blocking
result = await sem.try_acquire()
if result:
    try:
        await do_work()
    finally:
        sem.release()
else:
    print("Too busy, try later")

# Timeout (raises TimeoutError)
async with sem.acquire(timeout=5.0):
    await do_work()
```

## Key Methods

| Method | Description |
|---|---|
| `acquire()` | Wait for a permit (async) |
| `try_acquire()` | Non-blocking — returns permit or None |
| `acquire(timeout=)` | Wait with timeout |
| `acquire_many(n)` | Context manager for multiple permits |
| `release()` | Return one permit |
| `release_many(n)` | Return multiple permits |
| `get_stats()` | Introspection dict for agents |

## Real-World: LLM Rate Limiting

```python
from tiny_semaphore import Semaphore
import anthropic

# Anthropic caps at 10 concurrent requests
llm_sem = Semaphore(max_permits=10, name="anthropic")

async def call_claude(prompt: str, model: str = "claude-sonnet-4-20250514") -> str:
    async with llm_sem:
        response = await anthropic.AsyncAnthropic().messages.create(
            model=model,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text

# Fan out 500 prompts — only 10 run concurrently
results = await asyncio.gather(*[call_claude(p) for p in prompts])
```

## Real-World: Database Connection Pool

```python
from tiny_semaphore import Semaphore

# Your DB has 20 max connections
db_sem = Semaphore(max_permits=20, name="pg-pool")

async def query(sql: str):
    async with db_sem:
        result = await pool.execute(sql)
        return result

# With proper backpressure — never exceeds 20 connections
await asyncio.gather(*[query(sql) for sql in queries])
```

## Agent-Introspectable Stats

```python
sem.get_stats()
# {'name': 'anthropic',
#  'max_permits': 10,
#  'available': 7,
#  'acquired': 3,
#  'waiters': 2,
#  'peak_acquired': 10,
#  'total_releases': 500}
```

## Architecture

```
tiny-semaphore/
├── semaphore.py      # Core ~80 lines
├── pyproject.toml    # PyPI packaging
├── package.json      # npm/pip alternative
├── README.md
└── test_semaphore.py
```

## Comparison

| Feature | `tiny-semaphore` | `asyncio.Semaphore` | `aiolimits` |
|---|---|---|---|
| Lines of code | ~80 | built-in | ~200 |
| Dependencies | **0** | 0 | 1 |
| Timeout on acquire | ✅ | ❌ | ✅ |
| try_acquire | ✅ | ✅ | ✅ |
| Stats / introspection | ✅ | ❌ | ❌ |
| Batch acquire | ✅ | ❌ | ❌ |
| Fair FIFO | ✅ | ❌ | ❌ |

---

*Part of the [`tiny-*` ecosystem](https://github.com/hussain-alsaibai) — zero-dependency developer tools for AI-native applications.*
