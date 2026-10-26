from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

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
    service: ProcessServiceDep, params: Annotated[ListProcessesParams, Query()]
) -> ListProcessesResponse:
    """List all processes of the given datasets, paginated."""
    processes, total = await service.list_processes(params.datasets, params.limit, params.offset)
    return ListProcessesResponse(
        items=[Process.from_model(p) for p in processes],
        total=total,
        limit=params.limit,
        offset=params.offset,
    )


@router.get("/processes/{sgid}", summary="Get process")
async def get_process(service: ProcessServiceDep, sgid: sgid) -> ProcessResponse:
    """Get a process by sgid"""
    process = await service.get_process(sgid)
    if process is None:
        raise HTTPException(status_code=404, detail="Process not found")
    return ProcessResponse.from_model(process)
