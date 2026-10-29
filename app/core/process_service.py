import uuid
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

    async def list_processes(
        self, dataset_sgids: Sequence[uuid.UUID], limit: int, offset: int
    ) -> tuple[Sequence[Process], int]:
        """Return one page of processes of the given datasets and their total number."""
        in_datasets = Process.dataset_sgid.in_(dataset_sgids)
        processes = await self.session.scalars(
            select(Process).where(in_datasets).order_by(Process.name).limit(limit).offset(offset)
        )
        total = (
            await self.session.scalar(select(func.count()).select_from(Process).where(in_datasets))
            or 0
        )
        return processes.all(), total

    async def get_process(self, sgid: uuid.UUID) -> Process | None:
        """Return the process with the given sgid, or None if it doesn't exist."""
        return await self.session.scalar(select(Process).where(Process.sgid == sgid))

    async def get_datasets(
        self, dataset_sgids: Sequence[uuid.UUID]
    ) -> Sequence[tuple[uuid.UUID, uuid.UUID, uuid.UUID]]:
        """Get tuple of (dataset, unit, organization) of provided datasets."""
        rows = await self.session.execute(
            select(Process.dataset_sgid, Process.organization_unit_sgid, Process.organization_sgid)
            .where(Process.dataset_sgid.in_(dataset_sgids))
            .distinct()
        )
        return rows.tuples().all()


# FastAPI dependency injection function and definitions
def get_process_service(session: DatabaseSessionDep) -> ProcessService:
    return ProcessService(session)


ProcessServiceDep = Annotated[ProcessService, Depends(get_process_service)]
