import abc
from typing import List
from typing import Optional

from optjournal import _models


class Database(object, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create_study(self, study_name: str) -> _models.StudyModel:
        raise NotImplementedError

    @abc.abstractmethod
    def find_study(self, study_id: int) -> Optional[_models.StudyModel]:
        raise NotImplementedError

    @abc.abstractmethod
    def find_study_by_name(self, study_name: str) -> Optional[_models.StudyModel]:
        raise NotImplementedError

    @abc.abstractmethod
    def delete_study(self, study_id: int) -> Optional[_models.StudyModel]:
        raise NotImplementedError

    @abc.abstractmethod
    def get_all_studies(self) -> List[_models.StudyModel]:
        raise NotImplementedError

    @abc.abstractmethod
    def append_operations(self, ops: List[_models.OperationModel]) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def read_operations(self, study_id: int, next_op_id: int) -> List[_models.OperationModel]:
        raise NotImplementedError
