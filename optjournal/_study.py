from datetime import datetime
from typing import Any
from typing import Dict
from typing import Optional

import optuna
from optuna import distributions
from optuna.trial import TrialState

from optjournal._operation import _Operation

MAX_TRIAL_NUM = 1000000


class _Trial(optuna.trial.FrozenTrial):
    def __init__(
        self,
        number,  # type: int
        state,  # type: TrialState
        value,  # type: Optional[float]
        datetime_start,  # type: Optional[datetime.datetime]
        datetime_complete,  # type: Optional[datetime.datetime]
        params,  # type: Dict[str, Any]
        distributions,  # type: Dict[str, BaseDistribution]
        user_attrs,  # type: Dict[str, Any]
        system_attrs,  # type: Dict[str, Any]
        intermediate_values,  # type: Dict[int, float]
        trial_id,  # type: int
        owner: str,
    ) -> None:
        super().__init__(
            number=number,
            state=state,
            value=value,
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
        return self._trial_id // MAX_TRIAL_NUM


class _Study(object):
    def __init__(self, study_id: int) -> None:
        self.study_id = study_id
        self.next_op_id = 0
        self.trials = []
        self.direction = optuna.study.StudyDirection.NOT_SET

    def execute(self, op_id: int, kind: _Operation, data: Dict[str, Any]) -> None:
        self.next_op_id = op_id + 1

        if kind == _Operation.SET_STUDY_DIRECTION:
            self.direction = optuna.study.StudyDirection(data["direction"])
        elif kind == _Operation.CREATE_TRIAL:
            number = len(self.trials)
            trial_id = self.study_id * MAX_TRIAL_NUM + number

            trial = _Trial(
                trial_id=trial_id,
                number=number,
                state=TrialState.RUNNING,
                value=None,
                datetime_start=datetime.now(),  # TODO: data["datetime_start"]
                datetime_complete=None,
                params={},
                distributions={},
                user_attrs={},
                system_attrs={},
                intermediate_values={},
                owner=data["worker"],
            )
            self.trials.append(trial)
        elif kind == _Operation.SET_TRIAL_PARAM:
            # TODO: owner check
            number = data["trial_id"] % MAX_TRIAL_NUM
            name = data["name"]

            self.trials[number].params[name] = data["value"]
            self.trials[number].distributions[name] = distributions.json_to_distribution(
                data["distribution"]
            )
        elif kind == _Operation.SET_TRIAL_VALUE:
            # TODO: owner check
            number = data["trial_id"] % MAX_TRIAL_NUM
            self.trials[number].value = data["value"]
        elif kind == _Operation.SET_TRIAL_STATE:
            # TODO: owner check
            number = data["trial_id"] % MAX_TRIAL_NUM
            state = TrialState(data["state"])

            if state == TrialState.RUNNING and self.trials[number].state != TrialState.WAITING:
                return

            self.trials[number].state = state
            if state.is_finished():
                self.trials[
                    number
                ].datetime_complete = datetime.now()  # TODO: data["datetime_complete"]
            if state == TrialState.RUNNING:
                self.trials[number].owner = data["worker"]
        elif kind == _Operation.SET_TRIAL_SYSTEM_ATTR:
            number = data["trial_id"] % MAX_TRIAL_NUM
            self.trials[number].system_attrs[data["key"]] = data["value"]
        elif kind == _Operation.SET_TRIAL_INTERMEDIATE_VALUE:
            number = data["trial_id"] % MAX_TRIAL_NUM
            self.trials[number].intermediate_values[data["step"]] = data["value"]
        else:
            raise NotImplementedError("kind={}, data={}".format(kind, data))
