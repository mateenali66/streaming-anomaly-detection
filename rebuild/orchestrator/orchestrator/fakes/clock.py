"""Virtual clock. sleep() advances simulated time and drives the fake world, so a 70-hour campaign runs in seconds.

SIGTERM and SIGKILL are simulated at chosen instants:
  * SIGTERM sets the shutdown flag exactly as the real signal handler does; the next sleep raises ShutdownRequested.
  * SIGKILL raises ProcessDead, a BaseException that no orchestrator handler catches, and marks the world dead, so
    every later call into a fake adapter fails. A finally-block that tried to write state after the kill would
    therefore fail the test instead of silently making the simulation kinder than reality.
"""
from __future__ import annotations

from ..clock import Clock, ShutdownFlag


class ProcessDead(BaseException):
    """Simulated SIGKILL."""


class FakeClock(Clock):
    def __init__(self, start: float = 1_800_000_000.0) -> None:
        self.t = float(start)
        self.shutdown = ShutdownFlag()
        self.world = None
        self.kill_at: float | None = None
        self.term_at: float | None = None
        self.sleeps = 0

    def now(self) -> float:
        if self.world is not None and self.world.dead:
            raise ProcessDead("process is dead")
        return self.t

    def advance_to(self, t: float) -> None:
        if t > self.t:
            self.t = t
        if self.world is not None:
            self.world.advance(self.t)

    def sleep(self, seconds: float) -> None:
        if self.world is not None and self.world.dead:
            raise ProcessDead("process is dead")
        self.sleeps += 1
        self.check_shutdown()
        target = self.t + max(0.0, float(seconds))
        if self.kill_at is not None and target >= self.kill_at:
            self.advance_to(max(self.t, self.kill_at))
            self.kill_at = None
            if self.world is not None:
                self.world.dead = True
            raise ProcessDead(f"SIGKILL at {self.t}")
        if self.term_at is not None and target >= self.term_at and not self.shutdown.requested:
            self.advance_to(max(self.t, self.term_at))
            self.term_at = None
            self.shutdown.request("SIGTERM")
            self.check_shutdown()
        self.advance_to(target)

    def restart(self, delay_s: float = 30.0) -> None:
        """A new process: the shutdown flag is fresh and time moves on by the restart delay."""
        self.shutdown = ShutdownFlag()
        if self.world is not None:
            self.world.dead = False
        self.advance_to(self.t + delay_s)
