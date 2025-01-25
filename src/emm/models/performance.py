import enum
from datetime import datetime
from decimal import Decimal

from sqlalchemy import ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.emm.models.database_base import SQLBase
from src.emm.models.schema import Permutation, Schema


class EmmAnalysisType(enum.Enum):
    SIZE = "disk_size"
    PERFORMANCE_RO = "performance_ro"
    PERFORMANCE_RW = "performance_rw"


class Analysis(SQLBase):
    __tablename__ = "emm_analysis"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    name: Mapped[str]
    description: Mapped[str]
    type: Mapped[EmmAnalysisType]
    schema_id: Mapped[int] = mapped_column(ForeignKey("public.emm_project.id"))
    schema: Mapped[Schema] = relationship(Schema)
    created: Mapped[datetime] = mapped_column(
        insert_default=datetime.now(), default=None
    )


class AnalysisReport(SQLBase):
    __tablename__ = "emm_analysis_report"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    analysis_id: Mapped[int] = mapped_column(
        ForeignKey("public.emm_analysis.id", ondelete="CASCADE")
    )
    analysis: Mapped[Analysis] = relationship(Analysis)
    metric: Mapped[str]
    best_permutation_name: Mapped[str]
    improvement_percentage_over_baseline: Mapped[Decimal]
    size_table: Mapped[int]
    created: Mapped[datetime] = mapped_column(
        insert_default=datetime.now(), default=None
    )


class RawPerformanceRecord(SQLBase):
    __tablename__ = "emm_raw_performance"
    __table_args__ = {"schema": "public"}

    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    analysis_id: Mapped[int] = mapped_column(
        ForeignKey("public.emm_analysis.id", ondelete="CASCADE")
    )
    analysis: Mapped[Analysis] = relationship(Analysis)

    permutation_id: Mapped[int] = mapped_column(
        ForeignKey("public.emm_permutation.id", ondelete="CASCADE")
    )
    permutation: Mapped[Permutation] = relationship(Permutation, cascade="all")
    metric: Mapped[str]
    notes: Mapped[str]
    value: Mapped[int]
    created: Mapped[datetime] = mapped_column(
        insert_default=datetime.now(), default=None
    )
