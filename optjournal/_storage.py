from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import orm

from optuna.storages import BaseStorage

from optjournal._models import _BaseModel


class RDBJournalStorage(BaseStorage):
    def __init__(self, database_url: str) -> None:
        self._engine = create_engine(url)
        self._scoped_session = orm.scoped_session(orm.sessionmaker(bind=self._engine))
        _BaseModel.metadata.create_all(self._engine)

    def create_new_study(self, study_name: Optional[str] = None) -> int:
        raise NotImplementedError

    def delete_study(self, study_id: int) -> None:
        raise NotImplementedError

    def set_study_user_attr(self, study_id: int, key: str, value: Any) -> None:
        raise NotImplementedError

    def set_study_system_attr(self, study_id: int, key: str, value: Any) -> None:
        raise NotImplementedError

    def set_study_direction(self, study_id: int, direction: study.StudyDirection) -> None:
        raise NotImplementedError

    def get_study_id_from_name(self, study_name: str) -> int:
        raise NotImplementedError

    def get_study_id_from_trial_id(self, trial_id: int) -> int:
        raise NotImplementedError

    def get_study_name_from_id(self, study_id: int) -> str:
        raise NotImplementedError

    def get_study_direction(self, study_id: int) -> study.StudyDirection:
        raise NotImplementedError

    def get_study_user_attrs(self, study_id: int) -> Dict[str, Any]:
        raise NotImplementedError

    def get_study_system_attrs(self, study_id: int) -> Dict[str, Any]:
        raise NotImplementedError

    def get_all_study_summaries(self) -> List[study.StudySummary]:
        raise NotImplementedError

    def create_new_trial(
        self, study_id: int, template_trial: Optional["FrozenTrial"] = None
    ) -> int:
        raise NotImplementedError

    def set_trial_state(self, trial_id: int, state: TrialState) -> bool:
        raise NotImplementedError

    def set_trial_param(
        self,
        trial_id: int,
        param_name: str,
        param_value_internal: float,
        distribution: "distributions.BaseDistribution",
    ) -> bool:
        raise NotImplementedError

    def get_trial_number_from_id(self, trial_id: int) -> int:
        raise NotImplementedError

    def get_trial_param(self, trial_id: int, param_name: str) -> float:
        raise NotImplementedError

    def set_trial_value(self, trial_id: int, value: float) -> None:
        raise NotImplementedError

    def set_trial_intermediate_value(
        self, trial_id: int, step: int, intermediate_value: float
    ) -> bool:
        raise NotImplementedError

    def set_trial_user_attr(self, trial_id: int, key: str, value: Any) -> None:
        raise NotImplementedError

    def set_trial_system_attr(self, trial_id: int, key: str, value: Any) -> None:
        raise NotImplementedError

    def get_trial(self, trial_id: int) -> "FrozenTrial":
        raise NotImplementedError

    def get_all_trials(self, study_id: int, deepcopy: bool = True) -> List["FrozenTrial"]:
        raise NotImplementedError

    def get_n_trials(self, study_id: int, state: Optional[TrialState] = None) -> int:
        raise NotImplementedError

    def get_best_trial(self, study_id: int) -> "FrozenTrial":
        raise NotImplementedError

    def read_trials_from_remote_storage(self, study_id: int) -> None:
        raise NotImplementedError

    def remove_session(self) -> None:
        raise NotImplementedError
