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

_BaseModel = declarative_base()  # type: Any


class StudyModel(_BaseModel):
    __tablename__ = "studies"
    id = Column(Integer, primary_key=True)


class OperationModel(_BaseModel):
    __tablename__ = "operations"
    id = Column(Integer, primary_key=True)
    study_id = Column(Integer, ForeignKey("studies.id"), index=True, nullable=False)
    kind = Column(Enum(Operation), nullable=False)
    data = Column(LargeBinary, nullable=False)
