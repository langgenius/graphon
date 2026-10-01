from copy import deepcopy
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from graphon.model_runtime.v2 import (
    ContractRef,
    JobRef,
    JobStatus,
    JsonObject,
    JsonValue,
    ModelError,
    ModelRef,
    ModelResult,
)


def make_job_ref() -> JobRef:
    return JobRef(
        model=ModelRef(plugin_id="plugin", provider="provider", model="deployment"),
        contract=ContractRef(id="video", revision="1"),
        request_id="submission",
        id="remote-job",
        scope="configured",
        token={"input_ids": ["input-a"], "output_schema": {"type": "object"}},
    )


def make_job_result(job: JobRef) -> ModelResult:
    return ModelResult(
        request_id=job.request_id,
        model=job.model,
        contract=job.contract,
        output="media-reference",
    )


def test_job_handle_owns_submission_context_and_status_keeps_result_identity() -> None:
    input_ids: list[JsonValue] = ["input-a"]
    token: JsonObject = {"input_ids": input_ids}
    expected_token = deepcopy(token)
    job = JobRef(
        model=ModelRef(plugin_id="plugin", provider="provider", model="deployment"),
        contract=ContractRef(id="video", revision="1"),
        request_id="submission",
        id="remote-job",
        scope="configured",
        token=token,
    )
    input_ids.append("source mutation")
    assert job.token == expected_token
    units: list[JsonValue] = [1, None]
    usage: JsonObject = {"response": units}
    expiry = datetime(2030, 1, 1, tzinfo=UTC)
    status = JobStatus(
        job=job,
        status="succeeded",
        result=make_job_result(job),
        cancellable=False,
        expires_at=expiry,
        usage=usage,
    )
    assert status.result is not None
    assert status.result.request_id == "submission"
    assert status.result.contract == job.contract
    assert status.result.model == job.model
    assert status.expires_at == expiry
    units.append(2)
    assert status.usage == {"response": [1, None]}
    for record, field_name in (
        (job, "token"),
        (job, "request_id"),
        (status, "status"),
        (status, "job"),
    ):
        with pytest.raises(AttributeError):
            setattr(record, field_name, "changed")
    assert "input-a" not in repr(job)


@pytest.mark.parametrize(
    ("state", "has_result", "has_error", "cancellable"),
    [
        ("queued", True, False, False),
        ("queued", False, True, False),
        ("running", True, False, False),
        ("running", False, True, False),
        ("succeeded", False, False, False),
        ("succeeded", True, True, False),
        ("failed", False, False, False),
        ("failed", True, True, False),
        ("cancelled", True, False, False),
        ("cancelled", False, True, False),
        ("succeeded", True, False, True),
        ("failed", False, True, True),
        ("cancelled", False, False, True),
        ("unknown", False, False, False),
    ],
)
def test_job_status_rejects_conflicting_outcomes(
    state: Any, has_result: bool, has_error: bool, cancellable: bool
) -> None:
    job = make_job_ref()
    with pytest.raises((TypeError, ValueError)):
        JobStatus(
            job=job,
            status=state,
            result=make_job_result(job) if has_result else None,
            error=ModelError(code="provider_error", message="Failed")
            if has_error
            else None,
            cancellable=cancellable,
        )


@pytest.mark.parametrize("field_name", ["model", "contract", "request_id"])
def test_successful_job_rejects_a_result_from_another_submission(
    field_name: str,
) -> None:
    job = make_job_ref()
    result_fields: dict[str, Any] = {
        "model": job.model,
        "contract": job.contract,
        "request_id": job.request_id,
        "output": None,
    }
    result_fields[field_name] = {
        "model": ModelRef(plugin_id="other", provider="provider", model="deployment"),
        "contract": ContractRef(id="video", revision="2"),
        "request_id": "status-read",
    }[field_name]
    with pytest.raises((TypeError, ValueError)):
        JobStatus(
            job=job,
            status="succeeded",
            result=ModelResult(**result_fields),
            cancellable=False,
        )


@pytest.mark.parametrize(
    "usage", [None, {}, [], False, "", 0, 0.0, {"units": [None, False]}]
)
def test_status_usage_is_raw_and_independent_of_nested_result_usage(usage: Any) -> None:
    job = make_job_ref()
    result = ModelResult(
        request_id=job.request_id,
        model=job.model,
        contract=job.contract,
        output=None,
        usage={"completion": 4},
    )
    status = JobStatus(
        job=job, status="succeeded", result=result, cancellable=False, usage=usage
    )
    expected_usage = 0 if usage is None else usage
    assert status.usage == expected_usage
    assert type(status.usage) is type(expected_usage)
    assert status.result is not None
    assert status.result.usage == {"completion": 4}
    status_without_usage = JobStatus(
        job=job, status="succeeded", result=result, cancellable=False
    )
    assert status_without_usage.usage == 0
    assert type(status_without_usage.usage) is int
    assert status_without_usage.result is not None
    assert status_without_usage.result.usage == {"completion": 4}


@pytest.mark.parametrize("delay", [0, -1, float("nan"), float("inf"), True, "1"])
def test_job_status_requires_positive_finite_scheduling_hints(delay: Any) -> None:
    with pytest.raises((TypeError, ValueError)):
        JobStatus(
            job=make_job_ref(),
            status="queued",
            cancellable=True,
            next_check_after_seconds=delay,
        )


@pytest.mark.parametrize(
    "expiry",
    [
        datetime(2030, 1, 1, tzinfo=UTC).replace(tzinfo=None),
        datetime(2030, 1, 1, tzinfo=UTC).astimezone(timezone(timedelta(hours=1))),
    ],
)
def test_job_status_rejects_expiry_without_utc_timezone(expiry: datetime) -> None:
    with pytest.raises((TypeError, ValueError)):
        JobStatus(
            job=make_job_ref(), status="queued", cancellable=True, expires_at=expiry
        )


@pytest.mark.parametrize(
    "state", ["queued", "running", "succeeded", "failed", "cancelled"]
)
def test_job_status_accepts_each_exclusive_outcome(state: Any) -> None:
    job = make_job_ref()
    status = JobStatus(
        job=job,
        status=state,
        result=make_job_result(job) if state == "succeeded" else None,
        error=ModelError(code="provider_error", message="Failed")
        if state == "failed"
        else None,
        cancellable=state in {"queued", "running"},
    )
    assert status.status == state
    assert status.usage == 0
    assert type(status.usage) is int


def test_job_status_preserves_scheduling_hint() -> None:
    status = JobStatus(
        job=make_job_ref(),
        status="queued",
        cancellable=False,
        next_check_after_seconds=0.25,
    )
    assert status.next_check_after_seconds == 0.25  # ruff: ignore[float-equality-comparison]
    assert status.expires_at is None


@pytest.mark.parametrize("token", [None, False, "opaque", 0, [], {}])
def test_job_handle_preserves_opaque_json_tokens(token: Any) -> None:
    reference = make_job_ref()
    job = JobRef(
        model=reference.model,
        contract=reference.contract,
        request_id=reference.request_id,
        id=reference.id,
        scope=reference.scope,
        token=token,
    )
    assert job.token == token
    assert type(job.token) is type(token)
