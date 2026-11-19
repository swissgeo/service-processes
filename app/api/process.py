from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.exceptions import RequestValidationError

from app.core.authorization import (
    Action,
    AuthorizationDep,
    dataset_resource,
    process_resource,
)
from app.core.process_service import (
    IdempotencyKeyReusedError,
    InvalidInputsError,
    ProcessServiceDep,
)
from app.schemas.errors import ErrorResponse
from app.schemas.process import (
    IdempotencyKey,
    Job,
    JobResponse,
    ListJobsParams,
    ListJobsResponse,
    ListProcessesParams,
    ListProcessesResponse,
    Process,
    ProcessInput,
    ProcessResponse,
    SgidStr,
)

PROCESS_TAG = "Process"

router = APIRouter(tags=[PROCESS_TAG])


@router.get("/processes", summary="List all processes")
async def list_processes(
    service: ProcessServiceDep,
    authorization: AuthorizationDep,
    params: Annotated[ListProcessesParams, Query()],
) -> ListProcessesResponse:
    """List all processes of the given datasets the user may access, paginated."""
    owner_ids = await service.get_datasets(params.datasets)
    allowed = await authorization.filter(
        Action.LIST_PROCESSES, [dataset_resource(*ids) for ids in owner_ids]
    )
    processes, total = await service.list_processes(
        [dataset.id for dataset in allowed], params.limit, params.offset
    )
    return ListProcessesResponse(
        items=[Process.from_model(p) for p in processes],
        total=total,
        limit=params.limit,
        offset=params.offset,
    )


@router.get("/processes/{sgid}", summary="Get process")
async def get_process(
    sgid: SgidStr,
    service: ProcessServiceDep,
    authorization: AuthorizationDep,
) -> ProcessResponse:
    """Get a process by sgid"""
    process = await service.get_process(sgid)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    await authorization.require(Action.GET_PROCESS, process_resource(process))
    return ProcessResponse.from_model(process)


@router.post(
    "/processes/{sgid}/execution",
    summary="Start a process",
    status_code=201,
    responses={
        200: {"model": JobResponse, "description": "Job already started with this Idempotency-Key"},
        409: {
            "model": ErrorResponse,
            "description": "Idempotency-Key already used with a different request body",
        },
    },
)
# All arguments are injected by FastAPI, splitting them up would not make it simpler
async def execute_process(  # noqa: PLR0913, PLR0917
    sgid: SgidStr,
    body: ProcessInput,
    service: ProcessServiceDep,
    authorization: AuthorizationDep,
    response: Response,
    idempotency_key: IdempotencyKey,
) -> JobResponse:
    """Start a process by sgid.

    Returns 201 with the new job, or 200 with the existing job if a job was already started with
    the same Idempotency-Key for this process.
    """
    process = await service.get_process(sgid)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    await authorization.require(Action.EXECUTE_PROCESS, process_resource(process))

    try:
        job, created = await service.execute_process(process, body.inputs, idempotency_key)
    except InvalidInputsError as error:
        # Same response (400) as for other invalid request payloads
        raise RequestValidationError(error.errors) from None
    except IdempotencyKeyReusedError:
        raise HTTPException(
            status_code=409,
            detail="Idempotency-Key was already used with a different request body",
        ) from None
    if not created:
        response.status_code = 200
    return JobResponse.from_model(job)


@router.get("/jobs", summary="List jobs")
async def list_jobs(
    service: ProcessServiceDep,
    authorization: AuthorizationDep,
    params: Annotated[ListJobsParams, Query()],
) -> ListJobsResponse:
    """List the jobs of the given processes and/or datasets the user may access, newest first,
    paginated. At least one of `processes` or `datasets` is required, all filters must match.
    """
    # Get filtered by process and dataset query params
    processes = await service.get_processes(params.processes, params.datasets)
    # Check which of the processes the user is allowed to access
    allowed_processes = {
        resource.id
        for resource in await authorization.filter(
            Action.GET_PROCESS, [process_resource(p) for p in processes]
        )
    }
    allowed_process_ids = [p.id for p in processes if p.sgid in allowed_processes]
    jobs, total = await service.list_jobs(
        allowed_process_ids,
        params.status,
        params.limit,
        params.offset,
    )
    return ListJobsResponse(
        items=[Job.from_model(job) for job in jobs],
        total=total,
        limit=params.limit,
        offset=params.offset,
    )


@router.get("/jobs/{sgid}", summary="Get job details")
async def get_job(
    sgid: SgidStr,
    service: ProcessServiceDep,
    authorization: AuthorizationDep,
) -> JobResponse:
    """Get job details by sgid"""
    job = await service.get_job(sgid)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    await authorization.require(Action.GET_PROCESS, process_resource(job.process))

    job = await service.get_updated_job(job)
    return JobResponse.from_model(job)
