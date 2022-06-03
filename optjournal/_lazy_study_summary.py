from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
import uuid

import optuna

from optjournal import _models
from optjournal._study import _StudySummary


class LazyStudySummary(object):
    def __init__(self, study_id: int, study_name: str, storage: "JournalStorage") -> None:
        self.study_name = study_name
        self._study_id = study_id
        self._storage = storage
        self._summary = None

    @property
    def direction(self) -> optuna.study.StudyDirection:
        self._init_summary()
        return self._summary.direction

    @property
    def directions(self) -> List[optuna.study.StudyDirection]:
        self._init_summary()
        return self._summary.directions

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

        snapshot = self._storage._db.load_snapshot(self._study_id, "summary")
        if snapshot is None:
            study = _StudySummary(self._study_id)
        else:
            study = _StudySummary.deserialize(snapshot.data)

        worker_id = str(uuid.uuid4())
        ops = self._storage._db.read_operations(self._study_id, study.next_op_id)
        for op in ops:
            study.execute(op, worker_id)
        if len(ops) > 0:
            self._storage._db.save_snapshot(_models.SnapshotModel(
                study_id=self._study_id,
                name="summary",
                data=study.serialize()
            ))

        self._summary = optuna.study.StudySummary(
            study_name=self.study_name,
            study_id=self._study_id,
            direction=study.direction,
            directions=study.directions,
            best_trial=study.best_trial,
            user_attrs=study.user_attrs,
            system_attrs=study.system_attrs,
            n_trials=study.n_trials,
            datetime_start=study.datetime_start,
        )
