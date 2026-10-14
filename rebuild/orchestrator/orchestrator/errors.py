"""Exception types.

Two families. Ordinary failures (Exception subclasses) are caught by the attempt runner, recorded as a failed
check and still followed by recovery and cooldown. Campaign stops (BaseException subclasses) must not be caught
by any `except Exception` handler on the way up, so a halt or a SIGTERM can never be swallowed by an injector.
"""
from __future__ import annotations


class OrchestratorError(Exception):
    """Base class for every recoverable error. Never swallowed silently: callers record or re-raise it."""


class ConfigError(OrchestratorError):
    """Bad or missing configuration. Raised before anything is applied."""


class PlanMismatch(OrchestratorError):
    """The regenerated plan differs from the stored plan or from PLAN_SHA256. The orchestrator refuses to start."""


class QueryError(OrchestratorError):
    """A Prometheus, Flink REST or status-endpoint query failed. An empty result is not an error, but it is
    also never read as zero: callers that need a value treat empty as missing."""


class KubeError(OrchestratorError):
    """kubectl or the Kubernetes API failed for a reason other than NotFound."""


class StoreError(OrchestratorError):
    """An object-store (S3) operation failed."""


class InjectionError(OrchestratorError):
    """The fault could not be applied or could not be shown to have started."""


class SchemaError(OrchestratorError):
    """A record does not match the section 11 contract."""


class CampaignStop(BaseException):
    """Base for exits that must reach the top level untouched."""


class HaltRequested(CampaignStop):
    """A halt rule of protocol 2.6.5 fired."""

    def __init__(self, reason: str, attempt_key: str | None = None):
        super().__init__(reason)
        self.reason = reason
        self.attempt_key = attempt_key


class ShutdownRequested(CampaignStop):
    """SIGTERM or SIGINT was received."""
