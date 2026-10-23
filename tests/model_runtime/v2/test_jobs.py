import pytest

from graphon.model_runtime.v2 import (
    CallContext,
    ContractRef,
    JobRef,
    JobStatus,
    ModelCallError,
    ModelError,
    ModelJobs,
    ModelRef,
    ModelRequest,
    ModelResult,
)


class MediaJobs:
    def __init__(
        self,
        submission_status: JobStatus,
        status_response: JobStatus | ModelCallError,
        cancel_response: JobStatus | ModelCallError,
    ) -> None:
        self.submission_status = submission_status
        self.status_response = status_response
        self.cancel_response = cancel_response
        self.reads = 0

    def submit(self, request: ModelRequest, *, context: CallContext) -> JobStatus:
        assert request.model == self.submission_status.job.model
        assert request.contract == self.submission_status.job.contract
        assert context.request_id == self.submission_status.job.request_id
        return self.submission_status

    def get_status(self, job: JobRef, *, context: CallContext) -> JobStatus:
        assert job.scope == context.connection_id
        self.reads += 1
        if isinstance(self.status_response, ModelCallError):
            raise self.status_response
        return self.status_response

    def cancel(self, job: JobRef, *, context: CallContext) -> JobStatus:
        assert job.scope == context.connection_id
        if isinstance(self.cancel_response, ModelCallError):
            raise self.cancel_response
        return self.cancel_response


def as_jobs(jobs: ModelJobs) -> ModelJobs:
    # Consumer calls must retain the protocol type, rather than the narrowed fake.
    return jobs


def make_submission() -> tuple[ModelRequest, CallContext, JobRef]:
    request = ModelRequest(
        model=ModelRef(plugin_id="plugin", provider="provider", model="deployment"),
        contract=ContractRef(id="video", revision="1"),
        input={"input_id": "input-a"},
        parameters={"quality": "high"},
        output_schema={"type": "string"},
    )
    context = CallContext(connection_id="configured", request_id="submission")
    job = JobRef(
        model=request.model,
        contract=request.contract,
        request_id=context.request_id,
        id="remote-job",
        scope=context.connection_id,
        token={
            "input": request.input,
            "parameters": request.parameters,
            "output_schema": request.output_schema,
        },
    )
    return request, context, job


def test_job_only_implementation_reads_once_and_returns_the_latest_handle() -> None:
    request, context, job = make_submission()
    rotated = JobRef(
        model=job.model,
        contract=job.contract,
        request_id=job.request_id,
        id=job.id,
        scope=job.scope,
        token={"cursor": "rotated", "validation": job.token},
    )
    completed = JobStatus(
        job=rotated,
        status="succeeded",
        cancellable=False,
        result=ModelResult(
            request_id=job.request_id,
            model=job.model,
            contract=job.contract,
            output="media-reference",
        ),
    )
    queued = JobStatus(job=job, status="queued", cancellable=False)
    implementation = MediaJobs(
        submission_status=queued, status_response=completed, cancel_response=completed
    )
    jobs = as_jobs(implementation)
    submitted = jobs.submit(request, context=context)
    read_context = CallContext(
        connection_id=context.connection_id, request_id="status-read"
    )
    observed = jobs.get_status(submitted.job, context=read_context)
    assert implementation.reads == 1
    assert observed.job == rotated
    assert observed.job.token != submitted.job.token
    assert observed.result is not None
    assert observed.result.request_id == context.request_id
    assert observed.result.contract == request.contract
    assert jobs.cancel(observed.job, context=read_context) == observed
    immediate = as_jobs(
        MediaJobs(
            submission_status=completed,
            status_response=completed,
            cancel_response=completed,
        )
    )
    assert immediate.submit(request, context=context) == completed


@pytest.mark.parametrize("state", ["running", "succeeded", "cancelled"])
def test_plugin_cancellation_can_remain_pending_lose_race_or_confirm(
    state: str,
) -> None:
    request, context, job = make_submission()
    queued = JobStatus(job=job, status="queued", cancellable=True)
    if state == "succeeded":
        outcome = JobStatus(
            job=job,
            status="succeeded",
            cancellable=False,
            result=ModelResult(
                request_id=job.request_id,
                model=job.model,
                contract=job.contract,
                output="media-reference",
            ),
        )
    elif state == "cancelled":
        outcome = JobStatus(job=job, status="cancelled", cancellable=False)
    else:
        outcome = JobStatus(job=job, status="running", cancellable=True)
    jobs = as_jobs(
        MediaJobs(
            submission_status=queued, status_response=outcome, cancel_response=outcome
        )
    )
    submitted = jobs.submit(request, context=context)
    assert jobs.cancel(submitted.job, context=context).status == state


@pytest.mark.parametrize("code", ["not_found", "unavailable"])
def test_expired_handle_and_status_read_failure_are_call_errors(code: str) -> None:
    request, context, job = make_submission()
    queued = JobStatus(job=job, status="queued", cancellable=False)
    read_error = ModelCallError(
        error=ModelError(code=code, message="Cannot read status")
    )
    unsupported = ModelCallError(
        error=ModelError(
            code="unsupported_delivery", message="Cancellation unsupported"
        )
    )
    jobs = as_jobs(
        MediaJobs(
            submission_status=queued,
            status_response=read_error,
            cancel_response=unsupported,
        )
    )
    submitted = jobs.submit(request, context=context)
    with pytest.raises(ModelCallError) as caught:
        jobs.get_status(submitted.job, context=context)
    assert caught.value.error.code == code
    with pytest.raises(ModelCallError) as caught:
        jobs.cancel(submitted.job, context=context)
    assert caught.value.error.code == "unsupported_delivery"
    assert submitted.status == "queued"
