from typing import Protocol

from graphon.model_runtime.v2.application.context import CallContext
from graphon.model_runtime.v2.domain.exchange import ModelRequest
from graphon.model_runtime.v2.domain.job_status import JobRef, JobStatus


class ModelJobs(Protocol):
    def submit(self, request: ModelRequest, *, context: CallContext) -> JobStatus: ...

    def get_status(self, job: JobRef, *, context: CallContext) -> JobStatus: ...

    def cancel(self, job: JobRef, *, context: CallContext) -> JobStatus: ...
