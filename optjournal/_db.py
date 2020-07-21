from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple

import optuna
from sqlalchemy import asc
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import orm

from optjournal import _models
from optjournal._models import _BaseModel

MAX_RETRY_COUNT = 2


class _Database(object):
    def __init__(self, database_url: str) -> None:
        self._engine = create_engine(database_url)
        self._scoped_session = orm.scoped_session(orm.sessionmaker(bind=self._engine))
        _BaseModel.metadata.create_all(self._engine)

    def create_study(self, study_name: str) -> _models.StudyModel:
        return self._retry(lambda: self._create_study(study_name))

    def find_study(self, study_id: int) -> Optional[_models.StudyModel]:
        return self._retry(lambda: self._find_study(_models.StudyModel.id == study_id))

    def find_study_by_name(self, study_name: str) -> Optional[_models.StudyModel]:
        return self._retry(lambda: self._find_study(_models.StudyModel.name == study_name))

    def delete_study(self, study_id: int) -> Optional[_models.StudyModel]:
        return self._retry(lambda: self._delete_study(study_id))

    def append_operations(self, ops: List[_models.OperationModel]) -> None:
        self._retry(lambda: self._append_operations(ops))

    def read_operations(self, study_id: int, next_op_id: int) -> List[_models.OperationModel]:
        return self._retry(lambda: self._read_operations(study_id, next_op_id))

    def _create_study(self, study_name: str) -> _models.StudyModel:
        model = _models.StudyModel(name=study_name)
        session = self._scoped_session()
        session.add(model)
        session.commit()

        return model

    def _find_study(self, *conditions: Any) -> Optional[_models.StudyModel]:
        session = self._scoped_session()
        cls = _models.StudyModel
        model = session.query(cls).filter(*conditions).one_or_none()
        session.commit()
        return model

    def _delete_study(self, study_id: int) -> Optional[_models.StudyModel]:
        session = self._scoped_session()
        model = (
            session.query(_models.StudyModel)
            .filter(_models.Studymodel.id == study_id)
            .one_or_none()
        )
        if model is None:
            session.commit()
            return None

        session.delete(model)
        session.query(_models.OperationModel).filter(
            _models.OperationModel.study_id == study_id
        ).delete()
        session.commit()

        return model

    def _append_operations(self, ops: List[_models.OperationModel]) -> None:
        session = self._scoped_session()
        for model in ops:
            session.add(model)
        session.commit()

    def _read_operations(self, study_id: int, next_op_id: int) -> List[_models.OperationModel]:
        session = self._scoped_session()

        cls = _models.OperationModel
        models = (
            session.query(cls)
            .filter(cls.study_id == study_id, cls.id >= next_op_id)
            .order_by(asc(cls.id))
            .all()
        )
        session.commit()

        return models

    def _retry(self, func: Callable[[], Any], retry_count: int = 0) -> Any:
        try:
            return func()
        except:
            if retry_count >= MAX_RETRY_COUNT:
                raise

            retry_count += 1
            self._retry(func, retry_count)
