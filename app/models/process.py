from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Process(Base):
    """Process ORM example for testing DB migrations using Alembic."""

    __tablename__ = "process"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(30))
