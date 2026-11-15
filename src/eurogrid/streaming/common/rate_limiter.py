
import time
import threading


class RateLimiter:
    """
    Simple thread-safe rate limiter.

    Controls the minimum interval between requests.
    """

    def __init__(self, max_requests: int = 200, period: int = 60):
        if max_requests <= 0 or period <= 0:
            raise ValueError(
                "max_requests and period must be positive"
            )

        self.max_requests = max_requests
        self.period = period

        self.min_interval = period / max_requests

        self.last_request_time = 0.0
        self.lock = threading.Lock()

    def wait(self) -> None:
        """
        Wait before allowing the next request.
        """

        with self.lock:
            current_time = time.monotonic()

            elapsed = current_time - self.last_request_time

            wait_time = self.min_interval - elapsed

            if wait_time > 0:
                time.sleep(wait_time)

            self.last_request_time = time.monotonic()
