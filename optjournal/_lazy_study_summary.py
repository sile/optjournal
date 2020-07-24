from datetime import datetime
from typing import Any
from typing import Dict

import optuna


class LazyStudySummary(object):
    def __init__(self, study_id: int, study_name: str, storage: "RDBJournalStorage") -> None:
        self.study_name = study_name
        self._study_id = study_id
        self._storage = storage
        self._summary = None

    @property
    def direction(self) -> optuna.study.StudyDirection:
        self._init_summary()
        return self._summary.direction

    @property
    def best_trial(self) -> optuna.trial.FrozenTrial:
        self._init_summary()
        return self._summary.best_trial

    @property
    def user_attrs(self) -> Dict[str, Any]:
        self._init_summary()
        return self._summary.user_attrs

    @property
    def system_attrs(self) -> Dict[str, Any]:
        self._init_summary()
        return self._summary.system_attrs

    @property
    def n_trials(self) -> int:
        self._init_summary()
        return self._summary.n_trials

    @property
    def datetime_start(self) -> datetime:
        self._init_summary()
        return self._summary.datetime_start

    def _init_summary(self) -> None:
        if self._summary is not None:
            return

        self._storage._sync(self._study_id)
        study = self._storage._studies[self._study_id]
        self._summary = optuna.study.StudySummary(
            study_name=self.study_name,
            study_id=self._study_id,
            direction=study.direction,
            best_trial=study.best_trial,
            user_attrs=study.user_attrs,
            system_attrs=study.system_attrs,
            n_trials=len(study.trials),
            datetime_start=study.trials[0].datetime_start if study.trials else None,
        )

        del self._storage._studies[self._study_id]
