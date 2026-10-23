from collections.abc import Sequence
from typing import Annotated

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi import Depends

from app.core.database import DatabaseSessionDep
from app.models.process import Process


class ProcessService:
    """Reads and manages processes."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_processes(self, limit: int, offset: int) -> tuple[Sequence[Process], int]:
        """Return one page of processes and the total number of processes."""
        processes = await self.session.scalars(
            select(Process).order_by(Process.name).limit(limit).offset(offset)
        )
        total = await self.session.scalar(select(func.count()).select_from(Process)) or 0
        return processes.all(), total


# FastAPI dependency injection function and definitions
def get_process_service(session: DatabaseSessionDep) -> ProcessService:
    return ProcessService(session)


ProcessServiceDep = Annotated[ProcessService, Depends(get_process_service)]
