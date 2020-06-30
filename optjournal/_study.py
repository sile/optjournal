from typing import Any
from typing import Dict

import optuna

from optjournal._operation import _Operation


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
    ) -> None:
        pass


class _StudyState(object):
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
            raise NotImplementedError("kind={}, data={}".format(kind, data))
        else:
            raise NotImplementedError("kind={}, data={}".format(kind, data))
