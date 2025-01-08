from datetime import datetime

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.emm.models.database_base import SQLBase


class Schema(SQLBase):
    __tablename__ = "emm_project"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    name: Mapped[str]
    original_table_name: Mapped[str]
    # Define the relationship with cascade delete
    permutations: Mapped[list["Permutation"]] = relationship(
        "Permutation",
        back_populates="schema",
        cascade="all, delete-orphan",
        default_factory=list,
    )
    created: Mapped[datetime] = mapped_column(
        insert_default=datetime.now(), default=None
    )


class Permutation(SQLBase):
    __tablename__ = "emm_permutation"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    name: Mapped[str]
    is_populated: Mapped[bool]
    is_permutation: Mapped[bool]
    permutation_id: Mapped[int]
    schema_id: Mapped[int] = mapped_column(
        ForeignKey("public.emm_project.id", ondelete="CASCADE")
    )
    schema: Mapped[Schema] = relationship(Schema, back_populates="permutations")
    created: Mapped[datetime] = mapped_column(
        insert_default=datetime.now(), default=None
    )
