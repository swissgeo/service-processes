from typing import Annotated

from fastapi import APIRouter, Query

from app.core.process_service import ProcessServiceDep
from app.schemas.pagination import PaginationParams
from app.schemas.process import ListProcessesResponse, Process

PROCESS_TAG = "Process"

router = APIRouter(tags=[PROCESS_TAG])


@router.get("/processes", summary="List all processes")
async def list_processes(
    service: ProcessServiceDep, pagination: Annotated[PaginationParams, Query()]
) -> ListProcessesResponse:
    """List all processes, paginated."""
    processes, total = await service.list_processes(pagination.limit, pagination.offset)
    return ListProcessesResponse(
        items=[Process.from_model(p) for p in processes],
        total=total,
        limit=pagination.limit,
        offset=pagination.offset,
    )
