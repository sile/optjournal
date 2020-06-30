import copy
import json
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
import uuid

from sqlalchemy import asc
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import orm

import optuna
from optuna import study
from optuna.storages import BaseStorage
from optuna.trial import TrialState

from optjournal._operation import _Operation
from optjournal import _models
from optjournal._models import _BaseModel
from optjournal._study import _StudyState


class RDBJournalStorage(BaseStorage):
    def __init__(self, database_url: str) -> None:
        self._engine = create_engine(database_url)
        self._scoped_session = orm.scoped_session(orm.sessionmaker(bind=self._engine))
        _BaseModel.metadata.create_all(self._engine)

        self._studies = {}  # type: Dict[int, _StudyState]
        self._buffer = []  # type: List[_models.OperationModel]
        self._worker_id = str(uuid.uuid4())

    def create_new_study(self, study_name: Optional[str] = None) -> int:
        if study_name is None:
            study_name = str(uuid.uuid4())  # TODO: Align to Optuna's logic.

        model = _models.StudyModel(name=study_name)
        self._insert_model(model)
        return model.id

    def delete_study(self, study_id: int) -> None:
        cls = _models.StudyModel
        self._delete_model(cls, (cls.id == study_id,))

    def _sync(self, study_id) -> None:
        if study_id not in self._studies:
            self._studies[study_id] = _StudyState(study_id)

        session = self._scoped_session()
        try:
            for model in self._buffer:
                session.add(model)
            session.commit()
            self._buffer = []
        except Exception:
            session.rollback()
            raise

        session = self._scoped_session()
        try:
            cls = _models.OperationModel
            models = (
                session.query(cls)
                .filter(cls.study_id == study_id, cls.id >= self._studies[study_id].next_op_id)
                .order_by(asc(cls.id))
                .all()
            )
            session.commit()
        except Exception:
            session.rollback()
            raise

        for model in models:
            self._studies[study_id].execute(model.id, model.kind, json.loads(model.data))

    def _enqueue(self, study_id: int, kind: _Operation, data: Dict[str, Any]) -> None:
        model = _models.OperationModel(study_id=study_id, kind=kind, data=json.dumps(data))
        self._buffer.append(model)

    def set_study_user_attr(self, study_id: int, key: str, value: Any) -> None:
        self._enqueue(study_id, _Operation.SET_STUDY_USER_ATTR, {"key": key, "value": value})
        self._sync(study_id)

    def set_study_system_attr(self, study_id: int, key: str, value: Any) -> None:
        self._enqueue(study_id, _Operation.SET_STUDY_SYSTEM_ATTR, {"key": key, "value": value})
        self._sync(study_id)

    def set_study_direction(self, study_id: int, direction: study.StudyDirection) -> None:
        # TODO(sile): Validation.

        self._enqueue(study_id, _Operation.SET_STUDY_DIRECTION, {"direction": direction.value})
        self._sync(study_id)

    def get_study_id_from_name(self, study_name: str) -> int:
        cls = _models.StudyModel
        model = self._find_model(cls, (cls.name == study_name,))
        assert model is not None

        return model.id

    def get_study_id_from_trial_id(self, trial_id: int) -> int:
        raise NotImplementedError

    def get_study_name_from_id(self, study_id: int) -> str:
        cls = _models.StudyModel
        model = self._find_model(cls, (cls.id == study_id,))
        assert model is not None

        return model.name

    def get_study_direction(self, study_id: int) -> study.StudyDirection:
        if study_id in self._studies:
            if self._studies[study_id].direction != study.StudyDirection.NOT_SET:
                return self._studies[study_id].direction

        self._sync(study_id)
        return self._studies[study_id].direction

    def get_n_trials(self, study_id: int) -> int:
        raise NotImplementedError

    def get_study_user_attrs(self, study_id: int) -> Dict[str, Any]:
        self._sync(study_id)
        return self._studies[study_id].user_attrs

    def get_study_system_attrs(self, study_id: int) -> Dict[str, Any]:
        self._sync(study_id)
        return self._studies[study_id].system_attrs

    def get_all_study_summaries(self) -> List[study.StudySummary]:
        raise NotImplementedError

    def create_new_trial(
        self, study_id: int, template_trial: Optional["FrozenTrial"] = None
    ) -> int:
        if template_trial is not None:
            raise NotImplementedError

        data = {"worker": self._worker_id}
        self._enqueue(study_id, _Operation.CREATE_TRIAL, data)
        self._sync(study_id)

        trial = self._get_my_trial(study_id)
        assert trial is not None

        return trial.id

    def set_trial_state(self, trial_id: int, state: TrialState) -> bool:
        # TODO(sile): validate

        study_id = self._get_study_id(trial_id)
        data = {"trial_id": trial_id, "state": state.value, "worker": self._worker_id}
        self._enqueue(study_id, _Operation.SET_TRIAL_STATE, data)
        self._sync(study_id)

        trial, _ = self._get_trial(trial_id)
        if state == TrialState.RUNNING and trial.owner != self._worker_id:
            return False

        return True

    def set_trial_param(
        self,
        trial_id: int,
        param_name: str,
        param_value_internal: float,
        distribution: "distributions.BaseDistribution",
    ) -> bool:
        trial, study_id = self._get_trial(trial_id)
        if param_name in trial.params:
            return False

        trial.params[param_name] = param_value
        trial.distributios[param_name] = distributio

        data = {
            "trial_id": trial_id,
            "name": param_name,
            "value": param_value,
            "distribution": optuna.distributions.distribution_to_json(distribution),
        }
        self._enqueue(study_id, _Operation.SET_TRIAL_PARAM, data)
        return True

    def get_trial_number_from_id(self, trial_id: int) -> int:
        trial, _ = self._get_trial(trial_id)
        return trial.number

    def get_trial_param(self, trial_id: int, param_name: str) -> float:
        trial, _ = self._get_trial(trial_id)
        return trial.distributions[param_name].to_external_repr(trial.params[param_name])

    def set_trial_value(self, trial_id: int, value: float) -> None:
        # TODO(ohta): validation

        study_id = self._get_study_id(trial_id)
        data = {"trial_id": trial_id, "value": value}
        self._enqueue(study_id, _Operation.SET_TRIAL_VALUE, data)
        self._sync(study_id)

    def set_trial_intermediate_value(
        self, trial_id: int, step: int, intermediate_value: float
    ) -> bool:
        # TODO(ohta): validation

        study_id = self._get_study_id(trial_id)
        data = {"trial_id": trial_id, "value": value, "step": step}
        self._enqueue(study_id, _Operation.SET_TRIAL_INTERMEDIATE_VALUE, data)
        self._sync(study_id)

        return True

    def set_trial_user_attr(self, trial_id: int, key: str, value: Any) -> None:
        study_id = self._get_study_id(trial_id)
        data = {"trial_id": trial_id, "key": key, "value": value}
        self._enqueue(study_id, _Operation.SET_TRIAL_USER_ATTR, data)
        self._sync(study_id)

    def set_trial_system_attr(self, trial_id: int, key: str, value: Any) -> None:
        study_id = self._get_study_id(trial_id)
        data = {"trial_id": trial_id, "key": key, "value": value}
        self._enqueue(study_id, _Operation.SET_TRIAL_SYSTEM_ATTR, data)
        self._sync(study_id)

    def get_trial(self, trial_id: int) -> "FrozenTrial":
        trial, _ = self._get_trial(trial_id)
        return trial

    def get_all_trials(self, study_id: int, deepcopy: bool = True) -> List["FrozenTrial"]:
        if deepcopy:
            return copy.deepcopy(self._studies[study_id].trials)
        else:
            return self._studies[study_id].trials[:]

    # def get_best_trial(self, study_id: int) -> "FrozenTrial":
    #     raise NotImplementedError

    def read_trials_from_remote_storage(self, study_id: int) -> None:
        self._sync(study_id)

    # def remove_session(self) -> None:
    #     raise NotImplementedError

    def _insert_model(self, model: _BaseModel) -> None:
        try:
            session = self._scoped_session()
            session.add(model)
            session.commit()
        except Exception:
            session.rollback()
            raise

    def _find_model(self, cls: type, conditions: Tuple[Any, ...]) -> Optional[_BaseModel]:
        session = self._scoped_session()

        model = session.query(cls).filter(*conditions).one_or_none()

        try:
            session.commit()
            return model
        except Exception:
            session.rollback()
            raise

    def _update_model(
        self, cls: type, conditions: Tuple[Any, ...], update_fn: Callable[[_BaseModel], None]
    ):
        session = self._scoped_session()

        model = session.query(cls).filter(*conditions).with_for_update().one_or_none()
        if model is None:
            model = cls()
        update_fn(model)

        try:
            session.add(model)
            session.commit()
        except Exception:
            session.rollback()
            raise

    def _delete_model(self, cls: type, conditions: Tuple[Any, ...]):
        session = self._scoped_session()

        model = session.query(cls).filter(*conditions).one_or_none()
        if model is None:
            session.commit()
            return None

        try:
            session.delete(model)
            session.commit()
        except Exception:
            session.rollback()
            raise
