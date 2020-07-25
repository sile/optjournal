import copy
from datetime import datetime
import json
import threading
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union
import uuid

import optuna
from optuna import study
from optuna.storages import BaseStorage
from optuna.trial import TrialState
from sqlalchemy.exc import IntegrityError

from optjournal._database import Database
from optjournal import _id
from optjournal._lazy_study_summary import LazyStudySummary
from optjournal._operation import _Operation
from optjournal import _models
from optjournal._rdb import RDBDatabase
from optjournal._study import _Study


class JournalStorage(BaseStorage):
    def __init__(self, database: Union[str, Database]) -> None:
        if isinstance(database, str):
            self._db = RDBDatabase(database)
        else:
            self._db = database

        self._studies = {}  # type: Dict[int, _Study]
        self._buffered_ops = []  # type: List[_models.OperationModel]
        self._worker_ids = {}  # type: Dict[int, str]
        self._lock = threading.Lock()

    def create_new_study(self, study_name: Optional[str] = None) -> int:
        if study_name is None:
            study_name = str(uuid.uuid4())  # TODO: Align to Optuna's logic.

        try:
            study = self._db.create_study(study_name)
        except IntegrityError:
            raise optuna.exceptions.DuplicatedStudyError

        return study.id

    def delete_study(self, study_id: int) -> None:
        study = self._db.delete_study(study_id)
        if study is None:
            raise KeyError("No such study: id={}.".format(study_id))

        if study_id in self._studies:
            del self._studies[study_id]

    def set_study_user_attr(self, study_id: int, key: str, value: Any) -> None:
        self._enqueue_op(study_id, _Operation.SET_STUDY_USER_ATTR, {"key": key, "value": value})
        self._sync(study_id)

    def set_study_system_attr(self, study_id: int, key: str, value: Any) -> None:
        self._enqueue_op(study_id, _Operation.SET_STUDY_SYSTEM_ATTR, {"key": key, "value": value})
        self._sync(study_id)

    def set_study_direction(self, study_id: int, direction: study.StudyDirection) -> None:
        self._enqueue_op(study_id, _Operation.SET_STUDY_DIRECTION, {"direction": direction.value})
        self._sync(study_id)

        if self._studies[study_id].direction != direction:
            raise ValueError(
                "The direction of the study {} has already been set to {}, not {}.".format(
                    study_id, self._studies[study_id].direction, direction
                )
            )

    def get_study_id_from_name(self, study_name: str) -> int:
        study = self._db.find_study_by_name(study_name)
        if study is None:
            raise KeyError("No such study: name={}.".format(study_name))

        return study.id

    def get_study_id_from_trial_id(self, trial_id: int) -> int:
        return _id.get_study_id(trial_id)

    def get_study_name_from_id(self, study_id: int) -> str:
        study = self._db.find_study(study_id)
        if study is None:
            raise KeyError("No such study: id={}.".format(study_id))

        return study.name

    def get_study_direction(self, study_id: int) -> study.StudyDirection:
        if study_id in self._studies:
            if self._studies[study_id].direction != study.StudyDirection.NOT_SET:
                return self._studies[study_id].direction

        self._sync(study_id)
        return self._studies[study_id].direction

    def get_n_trials(self, study_id: int) -> int:
        return len(self.get_all_trials(study_id, deepcopy=False))

    def get_study_user_attrs(self, study_id: int) -> Dict[str, Any]:
        self._sync(study_id)
        return self._studies[study_id].user_attrs

    def get_study_system_attrs(self, study_id: int) -> Dict[str, Any]:
        self._sync(study_id)
        return self._studies[study_id].system_attrs

    def get_all_study_summaries(self) -> List["LazyStudySummary"]:
        return [
            LazyStudySummary(model.id, model.name, self) for model in self._db.get_all_studies()
        ]

    def create_new_trial(
        self, study_id: int, template_trial: Optional["FrozenTrial"] = None
    ) -> int:
        data = {"datetime_start": datetime.now().timestamp(), "worker_id": self._worker_id()}

        if template_trial is not None:
            data["state"] = template_trial.state.value
            if template_trial.value is not None:
                data["value"] = template_trial.value
            if template_trial.datetime_start is not None:
                data["datetime_start"] = template_trial.datetime_start.timestamp()
            if template_trial.datetime_complete is not None:
                data["datetime_complete"] = template_trial.datetime_complete.timestamp()
            if template_trial.params:
                data["params"] = template_trial.params
            if template_trial.distributions:
                data["distributions"] = template_trial.distributions
            if template_trial.user_attrs:
                data["user_attrs"] = template_trial.user_attrs
            if template_trial.system_attrs:
                data["system_attrs"] = template_trial.system_attrs
            if template_trial.intermediate_values:
                data["intermediate_values"] = template_trial.intermediate_values

        self._enqueue_op(study_id, _Operation.CREATE_TRIAL, data)
        self._sync(study_id)

        return self._studies[study_id].last_created_trial_ids[self._worker_id()]

    def set_trial_state(self, trial_id: int, state: TrialState) -> bool:
        study_id = _id.get_study_id(trial_id)
        data = {"trial_id": trial_id, "state": state.value, "worker_id": self._worker_id()}
        if state.is_finished():
            data["datetime_complete"] = datetime.now().timestamp()

        self._enqueue_op(study_id, _Operation.SET_TRIAL_STATE, data)
        self._sync(study_id)

        trial = self.get_trial(trial_id)
        if state == TrialState.RUNNING and trial.owner != self._worker_id():
            return False

        return True

    def set_trial_param(
        self,
        trial_id: int,
        param_name: str,
        param_value_internal: float,
        distribution: "distributions.BaseDistribution",
    ) -> bool:
        trial = self.get_trial(trial_id)
        if trial.owner != self._worker_id():
            raise RuntimeError
        if trial.state != TrialState.RUNNING:
            raise RuntimeError

        if param_name in trial.params:
            return False

        study_id = _id.get_study_id(trial_id)
        param_value = distribution.to_external_repr(param_value_internal)
        trial.params[param_name] = param_value
        trial.distributions[param_name] = distribution

        data = {
            "trial_id": trial_id,
            "name": param_name,
            "value": param_value,
            "distribution": optuna.distributions.distribution_to_json(distribution),
        }
        self._enqueue_op(study_id, _Operation.SET_TRIAL_PARAM, data)
        return True

    def get_trial_number_from_id(self, trial_id: int) -> int:
        return _id.get_trial_number(trial_id)

    def get_trial_param(self, trial_id: int, param_name: str) -> float:
        trial = self.get_trial(trial_id)
        return trial.distributions[param_name].to_external_repr(trial.params[param_name])

    def set_trial_value(self, trial_id: int, value: float) -> None:
        trial = self.get_trial(trial_id)
        if trial.owner != self._worker_id():
            raise RuntimeError
        if trial.state != TrialState.RUNNING:
            raise RuntimeError

        study_id = _id.get_study_id(trial_id)
        data = {"trial_id": trial_id, "value": value}
        self._enqueue_op(study_id, _Operation.SET_TRIAL_VALUE, data)
        # self._sync(study_id)

    def set_trial_intermediate_value(
        self, trial_id: int, step: int, intermediate_value: float
    ) -> bool:
        trial = self.get_trial(trial_id)
        if trial.owner != self._worker_id():
            raise RuntimeError
        if trial.state != TrialState.RUNNING:
            raise RuntimeError

        study_id = _id.get_study_id(trial_id)
        data = {"trial_id": trial_id, "value": intermediate_value, "step": step}
        self._enqueue_op(study_id, _Operation.SET_TRIAL_INTERMEDIATE_VALUE, data)
        self._sync(study_id)

        return True

    def set_trial_user_attr(self, trial_id: int, key: str, value: Any) -> None:
        trial = self.get_trial(trial_id)
        if trial.owner != self._worker_id():
            raise RuntimeError
        if trial.state != TrialState.RUNNING:
            raise RuntimeError

        study_id = _id.get_study_id(trial_id)
        data = {"trial_id": trial_id, "key": key, "value": value}
        self._enqueue_op(study_id, _Operation.SET_TRIAL_USER_ATTR, data)
        self._sync(study_id)

    def set_trial_system_attr(self, trial_id: int, key: str, value: Any) -> None:
        trial = self.get_trial(trial_id)
        if trial.owner != self._worker_id():
            raise RuntimeError
        if trial.state != TrialState.RUNNING:
            raise RuntimeError

        study_id = _id.get_study_id(trial_id)
        data = {"trial_id": trial_id, "key": key, "value": value}
        self._enqueue_op(study_id, _Operation.SET_TRIAL_SYSTEM_ATTR, data)
        self._sync(study_id)

    def get_trial(self, trial_id: int) -> "FrozenTrial":
        study_id = _id.get_study_id(trial_id)
        return self._studies[study_id].trials[_id.get_trial_number(trial_id)]

    def get_all_trials(self, study_id: int, deepcopy: bool = True) -> List["FrozenTrial"]:
        if study_id not in self._studies:
            self._sync(study_id)

        with self._lock:
            if deepcopy:
                return copy.deepcopy(self._studies[study_id].trials)
            else:
                return self._studies[study_id].trials[:]

    def get_best_trial(self, study_id: int) -> "FrozenTrial":
        return self._studies[study_id].best_trial

    def read_trials_from_remote_storage(self, study_id: int) -> None:
        self._sync(study_id)

    def _sync(self, study_id) -> None:
        with self._lock:
            if study_id not in self._studies:
                if self._db.find_study(study_id) is None:
                    raise KeyError("No such study: id={}.".format(study_id))

                self._studies[study_id] = _Study(study_id)

            # Write operations.
            self._db.append_operations(self._buffered_ops)
            self._buffered_ops = []

            # Read operations.
            ops = self._db.read_operations(study_id, self._studies[study_id].next_op_id)
            for op in ops:
                self._studies[study_id].execute(op, self._worker_id())

    def _enqueue_op(self, study_id: int, kind: _Operation, data: Dict[str, Any]) -> None:
        data = json.dumps([kind.value, data])
        with self._lock:
            last_op = self._buffered_ops[-1] if self._buffered_ops else None
            if (
                last_op is not None
                and last_op.study_id == study_id
                and len(last_op.data) + len(data) < 4096
            ):
                last_op.data = "{},{}".format(last_op.data[:-1], data[1:])
            else:
                model = _models.OperationModel(study_id=study_id, data=data)
                self._buffered_ops.append(model)

    # Lock-free internal methods.
    def _worker_id(self) -> str:
        if threading.get_ident() not in self._worker_ids:
            self._worker_ids[threading.get_ident()] = str(uuid.uuid4())

        return self._worker_ids[threading.get_ident()]
