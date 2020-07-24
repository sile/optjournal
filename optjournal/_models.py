from datetime import datetime
from typing import Any

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Enum
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import LargeBinary
from sqlalchemy import String
from sqlalchemy import UniqueConstraint

from optjournal._operation import _Operation

_BaseModel = declarative_base()  # type: Any


class StudyModel(_BaseModel):
    __tablename__ = "optjournal_studies"
    __table_args__ = (UniqueConstraint("name"),)
    id = Column(Integer, primary_key=True)
    name = Column(String(256), index=True, nullable=False)


class OperationModel(_BaseModel):
    __tablename__ = "optjournal_operations"
    id = Column(Integer, primary_key=True)
    study_id = Column(Integer, ForeignKey("optjournal_studies.id"), index=True, nullable=False)
    data = Column(String(4096), nullable=False)
