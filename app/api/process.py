from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from app.core.authorization import Action, AuthorizationDep, dataset_resource, process_resource
from app.core.process_service import ProcessServiceDep
from app.schemas.process import (
    ListProcessesParams,
    ListProcessesResponse,
    Process,
    ProcessResponse,
    sgid,
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
    sgid: sgid, service: ProcessServiceDep, authorization: AuthorizationDep
) -> ProcessResponse:
    """Get a process by sgid"""
    process = await service.get_process(sgid)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    await authorization.require(Action.GET_PROCESS, process_resource(process))
    return ProcessResponse.from_model(process)
