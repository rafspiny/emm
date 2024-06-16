from datetime import datetime

from sqlalchemy.orm import Mapped, mapped_column

from src.emm.models.database_base import SQLBase


class Schema(SQLBase):
    __tablename__ = "emm_schema"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    name: Mapped[str]
    is_populated: Mapped[bool]
    is_permutation: Mapped[bool]
    permutation_id: Mapped[int]
    created: Mapped[datetime] = mapped_column(
        insert_default=datetime.now(), default=None
    )
