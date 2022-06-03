from datetime import datetime
import json
import pickle
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import optuna
from optuna import distributions
from optuna.trial import TrialState

from optjournal import _id
from optjournal import _models
from optjournal._operation import _Operation


class _Trial(optuna.trial.FrozenTrial):
    def __init__(
        self,
        number,  # type: int
        state,  # type: TrialState
        values,  # type: Optional[List[float]]
        datetime_start,  # type: Optional[datetime.datetime]
        datetime_complete,  # type: Optional[datetime.datetime]
        params,  # type: Dict[str, Any]
        distributions,  # type: Dict[str, BaseDistribution]
        user_attrs,  # type: Dict[str, Any]
        system_attrs,  # type: Dict[str, Any]
        intermediate_values,  # type: Dict[int, float]
        trial_id,  # type: int
        owner: Optional[str],
    ) -> None:
        super().__init__(
            number=number,
            state=state,
            value=None,
            values=values,
            datetime_start=datetime_start,
            datetime_complete=datetime_complete,
            params=params,
            distributions=distributions,
            user_attrs=user_attrs,
            system_attrs=system_attrs,
            intermediate_values=intermediate_values,
            trial_id=trial_id,
        )

        self.owner = owner

    @property
    def study_id(self) -> int:
        return _id.get_study_id(self._trial_id)


class _Study(object):
    def __init__(self, study_id: int) -> None:
        self.study_id = study_id
        self.next_op_id = 0
        self.trials = []
        self.directions = []  # type: List[optuna.study.StudyDirection]
        self.user_attrs = {}  # type: Dict[str,Any]
        self.system_attrs = {}  # type: Dict[str,Any]
        self.best_trial = None  # type: Optional[optuna.trial.FrozenTrial]
        self.last_created_trial_ids = {}  # type: Dict[str,int]

    @property
    def direction(self) -> optuna.study.StudyDirection:
        return self.directions[0]

    def execute(self, op: _models.OperationModel, worker_id: str) -> None:
        self.next_op_id = op.id + 1

        items = json.loads(op.data)
        n = len(items)
        for i in range(n // 2):
            kind, data = items[i * 2], items[i * 2 + 1]
            kind = _Operation(kind)

            if kind == _Operation.SET_STUDY_DIRECTIONS:
                self._set_study_directions(data, worker_id)
            elif kind == _Operation.CREATE_TRIAL:
                self._create_trial(data, worker_id)
            elif kind == _Operation.SET_TRIAL_PARAM:
                self._set_trial_param(data, worker_id)
            elif kind == _Operation.SET_TRIAL_VALUES:
                self._set_trial_values(data, worker_id)
            elif kind == _Operation.SET_TRIAL_STATE:
                self._set_trial_state(data, worker_id)
            elif kind == _Operation.SET_TRIAL_SYSTEM_ATTR:
                self._set_trial_system_attr(data, worker_id)
            elif kind == _Operation.SET_TRIAL_USER_ATTR:
                self._set_trial_system_attr(data, worker_id)
            elif kind == _Operation.SET_TRIAL_INTERMEDIATE_VALUE:
                self._set_trial_intermediate_value(data, worker_id)
            elif kind == _Operation.SET_STUDY_USER_ATTR:
                self._set_study_user_attr(data, worker_id)
            elif kind == _Operation.SET_STUDY_SYSTEM_ATTR:
                self._set_study_system_attr(data, worker_id)
            else:
                raise NotImplementedError("kind={}, data={}".format(kind, data))

    def _set_study_directions(self, data: Dict[str, Any], worker_id: str) -> None:
        self.directions = [optuna.study.StudyDirection(d) for d in data["directions"]]

    def _create_trial(self, data: Dict[str, Any], worker_id: str) -> None:
        number = len(self.trials)
        trial_id = _id.make_trial_id(self.study_id, number)

        if "datetime_complete" in data:
            data["datetime_complete"] = datetime.fromtimestamp(data["datetime_complete"])

        state = TrialState(data.get("state", TrialState.RUNNING.value))

        owner = None
        if state == TrialState.RUNNING:
            owner = data["worker_id"]

        trial = _Trial(
            trial_id=trial_id,
            number=number,
            state=state,
            values=data.get("values"),
            datetime_start=datetime.fromtimestamp(data["datetime_start"]),
            datetime_complete=data.get("datetime_complete"),
            params=data.get("params", {}),
            distributions=data.get("distributions", {}),
            user_attrs=data.get("user_attrs", {}),
            system_attrs=data.get("system_attrs", {}),
            intermediate_values=data.get("intermediate_values", {}),
            owner=owner,
        )
        self.trials.append(trial)

        if data["worker_id"] == worker_id:
            self.last_created_trial_ids[worker_id] = trial_id

    def _set_trial_state(self, data: Dict[str, Any], worker_id: str) -> None:
        number = _id.get_trial_number(data["trial_id"])
        trial = self.trials[number]

        state = TrialState(data["state"])
        if state == TrialState.RUNNING:
            if trial.owner != data["worker_id"]:
                if data["worker_id"] == worker_id:
                    raise RuntimeError(
                        "Trial {} cannot be modified from the owner.".format(number)
                    )
                else:
                    return

            if self.trials[number].state != TrialState.WAITING:
                return

        if trial.state.is_finished():
            if data["worker_id"] == worker_id:
                raise RuntimeError("Trial {} has already been finished.".format(number))
            else:
                return

        trial.state = state
        if state.is_finished():
            trial.datetime_complete = datetime.fromtimestamp(data["datetime_complete"])
            trial.owner = None

        if state == TrialState.RUNNING:
            self.trials[number].owner = data["worker_id"]

        if state == TrialState.COMPLETE:
            if (
                self.best_trial is None
                or (
                    self.direction == optuna.study.StudyDirection.MINIMIZE
                    and trial.value < self.best_trial.value
                )
                or (
                    self.direction == optuna.study.StudyDirection.MAXIMIZE
                    and trial.value > self.best_trial.value
                )
            ):
                self.best_trial = trial

    def _set_trial_param(self, data: Dict[str, Any], worker_id: str) -> None:
        number = _id.get_trial_number(data["trial_id"])
        name = data["name"]

        self.trials[number].params[name] = data["value"]
        self.trials[number].distributions[name] = distributions.json_to_distribution(
            data["distribution"]
        )

    def _set_trial_values(self, data: Dict[str, Any], worker_id: str) -> None:
        number = _id.get_trial_number(data["trial_id"])
        self.trials[number].values = data["values"]

    def _set_trial_intermediate_value(self, data: Dict[str, Any], worker_id: str) -> None:
        number = _id.get_trial_number(data["trial_id"])
        self.trials[number].intermediate_values[data["step"]] = data["value"]

    def _set_trial_system_attr(self, data: Dict[str, Any], worker_id: str) -> None:
        number = _id.get_trial_number(data["trial_id"])
        self.trials[number].system_attrs[data["key"]] = data["value"]

    def _set_trial_user_attr(self, data: Dict[str, Any], worker_id: str) -> None:
        number = _id.get_trial_number(data["trial_id"])
        self.trials[number].user_attrs[data["key"]] = data["value"]

    def _set_study_user_attr(self, data: Dict[str, Any], worker_id: str) -> None:
        self.user_attrs[data["key"]] = data["value"]

    def _set_study_system_attr(self, data: Dict[str, Any], worker_id: str) -> None:
        self.system_attrs[data["key"]] = data["value"]


class _StudySummary(_Study):
    def __init__(self, study_id: int) -> None:
        super().__init__(study_id)
        self.n_trials = 0
        self.datetime_start = None  # type: Optuna[datetime.Datetime]
        self.trials = {}

    def _create_trial(self, data: Dict[str, Any], worker_id: str) -> None:
        number = self.n_trials
        trial_id = _id.make_trial_id(self.study_id, number)

        if "datetime_complete" in data:
            data["datetime_complete"] = datetime.fromtimestamp(data["datetime_complete"])

        if self.datetime_start is None:
            self.datetime_start = datetime.fromtimestamp(data["datetime_start"])

        state = TrialState(data.get("state", TrialState.RUNNING.value))

        owner = None
        if state == TrialState.RUNNING:
            owner = data["worker_id"]

        trial = _Trial(
            trial_id=trial_id,
            number=number,
            state=state,
            values=data.get("values"),
            datetime_start=datetime.fromtimestamp(data["datetime_start"]),
            datetime_complete=data.get("datetime_complete"),
            params=data.get("params", {}),
            distributions=data.get("distributions", {}),
            user_attrs=data.get("user_attrs", {}),
            system_attrs=data.get("system_attrs", {}),
            intermediate_values=data.get("intermediate_values", {}),
            owner=owner,
        )
        self.trials[number] = trial
        self.n_trials += 1

    def _set_trial_state(self, data: Dict[str, Any], worker_id: str) -> None:
        super()._set_trial_state(data, worker_id)
        state = TrialState(data["state"])
        if state.is_finished():
            number = _id.get_trial_number(data["trial_id"])
            del self.trials[number]

    def serialize(self) -> bytes:
        # FIXME: Don't use pickle
        return pickle.dumps(self)

    def deserialize(data: bytes) -> '_StudySummary':
        # FIXME: Don't use pickle
        return pickle.loads(data)
