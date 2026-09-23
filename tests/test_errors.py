import logging

from app.telemetry.errors import (
    DependencyUnavailableError,
    UpstreamError,
    normalize,
)

log = logging.getLogger("test.errors")


def test_normalize_distinguishes_reasons():
    cases = [
        (TimeoutError("timed out after 1s"), "timeout", 504),
        (TimeoutError("os timeout"), "timeout", 504),
        (DependencyUnavailableError("pg: connection refused"), "dependency_unavailable", 503),
        (UpstreamError("billing 503", status_code=503), "upstream_5xx", 502),
        (UpstreamError("billing 400", status_code=400), "upstream_error", 502),
        (ConnectionError("reset by peer"), "connection_failed", 503),
        (ValueError("secret internal detail"), "internal", 500),
    ]
    for exc, reason, status in cases:
        err = normalize(exc)
        assert err.reason == reason
        assert err.http_status == status
        # 内部细节保留在 log_detail 中用于定位
        assert type(exc).__name__ in err.log_detail
        log.info(
            "reason=%s http=%d 判定依据=%s", err.reason, err.http_status, err.log_detail
        )


def test_internal_details_not_in_reason():
    err = normalize(ValueError("db password=hunter2 at 10.0.0.1"))
    assert "hunter2" not in err.reason
    assert "10.0.0.1" not in err.reason
    assert err.reason == "internal"
