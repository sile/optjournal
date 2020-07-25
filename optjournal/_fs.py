import fcntl
import io
import json
import os
from pathlib import Path
import shutil
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


class _FileSystemDatabase(object):
    def __init__(self, root_dir: str) -> None:
        self._root_dir = Path(root_dir)
        self._root_dir.mkdir(parents=True, exist_ok=True)
        if not self._root_dir.joinpath("metadata.json").exists():
            with open(self._root_dir.joinpath("metadata.json"), "w") as f:
                json.dump({"next_study_id": 0, "studies": {}}, f)

        self._files = {}

    def create_study(self, study_name: str) -> _models.StudyModel:
        with open(self._root_dir.joinpath("metadata.json"), "r+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            metadata = json.load(f)
            if study_name in metadata["studies"]:
                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                raise optuna.exceptions.DuplicatedStudyError()

            study_id = metadata["next_study_id"]
            metadata["next_study_id"] += 1
            metadata["studies"][study_name] = study_id

            f.seek(0)
            json.dump(metadata, f)
            f.flush()

            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

            self._root_dir.joinpath(str(study_id)).mkdir()

            return _models.StudyModel(id=study_id, name=study_name)

    def find_study(self, study_id: int) -> Optional[_models.StudyModel]:
        with open(self._root_dir.joinpath("metadata.json"), "r") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            metadata = json.load(f)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

            for name, id in metadata["studies"].items():
                if id == study_id:
                    return _models.StudyModel(id=study_id, name=name)

            return None

    def find_study_by_name(self, study_name: str) -> Optional[_models.StudyModel]:
        with open(self._root_dir.joinpath("metadata.json"), "r") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            metadata = json.load(f)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

            if study_name not in metadata["studies"]:
                return None

            study_id = metadata["studies"][study_name]
            return _models.StudyModel(id=study_id, name=study_name)

    def delete_study(self, study_id: int) -> Optional[_models.StudyModel]:
        with open(self._root_dir.joinpath("metadata.json"), "r+") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            metadata = json.load(f)

            for study_name, id in metadata["studies"].items():
                if id == study_id:
                    del metadata["studies"][study_name]

                    f.seek(0)
                    json.dump(metadata, f)
                    break

            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

            path = str(self._root_dir.joinpath(str(study_id)))
            shutil.rmtree(path)

            if study_id in self._files:
                self._files[study_id].close()
                del self._files[study_id]

            return _models.StudyModel(id=study_id, name=study_name)

    def get_all_studies(self) -> List[_models.StudyModel]:
        with open(self._root_dir.joinpath("metadata.json"), "r") as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_SH)
            metadata = json.load(f)
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

            return [
                _models.StudyModel(id=id, name=name) for name, id in metadata["studies"].items()
            ]

    def _get_journal_file(self, study_id: int):  # TODO: return type
        if study_id not in self._files:
            path = self._root_dir.joinpath(str(study_id)).joinpath("journal.json")
            self._files[study_id] = open(path, "a+")

        return self._files[study_id]

    def append_operations(self, ops: List[_models.OperationModel]) -> None:
        study_ops = {}
        for op in ops:
            if op.study_id not in study_ops:
                study_ops[op.study_id] = []

            study_ops[op.study_id].append(op)

        for study_id, ops in study_ops.items():
            f = self._get_journal_file(study_id)
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)

            f.seek(0, io.SEEK_END)
            for op in ops:
                f.write(op.data)
                f.write("\n")
            f.flush()
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)

    def read_operations(self, study_id: int, next_op_id: int) -> List[_models.OperationModel]:
        # Don't have to acquire lock here.
        f = self._get_journal_file(study_id)

        f.seek(next_op_id)
        ops = []
        while True:
            line = f.readline()
            if line[-1:] != "\n":
                break

            try:
                data = json.loads(line)
                ops.append(
                    _models.OperationModel(
                        id=f.tell() - 1, study_id=study_id, data=json.dumps(data)
                    )
                )
            except:
                print("# JSON: {}".format(line))
                raise

        return ops
