"""Clock abstraction. The real clock sleeps in slices so a SIGTERM is noticed within one second."""
from __future__ import annotations

import math
import time

from .errors import ShutdownRequested


class ShutdownFlag:
    def __init__(self) -> None:
        self.requested = False
        self.signal_name: str | None = None
        self.suppress = False  # set while the stop path cleans up, so its own sleeps are not interrupted

    def request(self, name: str) -> None:
        self.requested = True
        self.signal_name = name


class Clock:
    """Interface: now() returns epoch seconds UTC as a float, sleep(s) blocks."""

    def now(self) -> float:  # pragma: no cover - interface
        raise NotImplementedError

    def sleep(self, seconds: float) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def sleep_until(self, t: float) -> None:
        delta = t - self.now()
        if delta > 0:
            self.sleep(delta)

    def check_shutdown(self) -> None:
        flag = getattr(self, "shutdown", None)
        if flag is not None and flag.requested and not flag.suppress:
            raise ShutdownRequested(flag.signal_name or "SIGTERM")


class RealClock(Clock):
    def __init__(self, shutdown: ShutdownFlag | None = None) -> None:
        self.shutdown = shutdown or ShutdownFlag()

    def now(self) -> float:
        return time.time()

    def sleep(self, seconds: float) -> None:
        end = time.time() + max(0.0, seconds)
        while True:
            self.check_shutdown()
            left = end - time.time()
            if left <= 0:
                return
            time.sleep(min(1.0, left))


def floor10(t: float) -> int:
    return int(math.floor(t / 10.0) * 10)


def ceil10(t: float) -> int:
    return int(math.ceil(t / 10.0) * 10)
