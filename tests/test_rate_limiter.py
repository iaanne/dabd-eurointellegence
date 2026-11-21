from __future__ import annotations

import time
import pytest
from eurogrid.streaming.common.rate_limiter import RateLimiter


def test_rate_limiter_validasi_input() -> None:
    with pytest.raises(ValueError):
        RateLimiter(max_requests=0, period=60)
    with pytest.raises(ValueError):
        RateLimiter(max_requests=10, period=0)


def test_rate_limiter_wait() -> None:
    limiter = RateLimiter(max_requests=10, period=1)  # min_interval = 0.1s
    start = time.monotonic()
    limiter.wait()
    limiter.wait()
    elapsed = time.monotonic() - start
    assert elapsed >= 0.08  # Toleransi waktu eksekusi minimal

