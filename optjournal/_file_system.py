import fcntl
import io
import json
import os
from pathlib import Path
import random
import shutil
import time
from typing import Any
from typing import Callable
from typing import List
from typing import Optional
from typing import Tuple
import uuid

import optuna
from sqlalchemy import asc
from sqlalchemy.engine import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy import orm

from optjournal._database import Database
from optjournal import _models


class FileSystemDatabase(Database):
    def __init__(self, root_dir: str, avoid_flock: bool = False) -> None:
        self._root_dir = Path(root_dir)
        self._root_dir.mkdir(parents=True, exist_ok=True)
        if avoid_flock:
            self._file_lock = LinkLockCreator()
        else:
            self._file_lock = FcntlLock

        if not self._index_path().exists():
            with self._file_lock(open(self._index_path(), "w")) as f:
                json.dump({"next_study_id": 0, "studies": {}}, f)

        self._files = {}

    def create_study(self, study_name: str) -> _models.StudyModel:
        with self._file_lock(open(self._index_path(), "r+")) as f:
            index = json.load(f)
            if study_name in index["studies"]:
                raise optuna.exceptions.DuplicatedStudyError()

            study_id = index["next_study_id"]
            index["next_study_id"] += 1
            index["studies"][study_name] = study_id

            f.seek(0)
            json.dump(index, f)

            self._journal_path(study_id).parent.mkdir()

            return _models.StudyModel(id=study_id, name=study_name)

    def find_study(self, study_id: int) -> Optional[_models.StudyModel]:
        with self._file_lock(open(self._index_path(), "r"), readonly=True) as f:
            index = json.load(f)

            for name, id in index["studies"].items():
                if id == study_id:
                    return _models.StudyModel(id=id, name=name)

            return None

    def find_study_by_name(self, study_name: str) -> Optional[_models.StudyModel]:
        with self._file_lock(open(self._index_path(), "r"), readonly=True) as f:
            index = json.load(f)
            if study_name not in index["studies"]:
                return None

            study_id = index["studies"][study_name]
            return _models.StudyModel(id=study_id, name=study_name)

    def delete_study(self, study_id: int) -> Optional[_models.StudyModel]:
        with self._file_lock(open(self._index_path(), "r+")) as f:
            index = json.load(f)

            for study_name, id in index["studies"].items():
                if id == study_id:
                    del index["studies"][study_name]

                    f.seek(0)
                    json.dump(index, f)
                    break

            shutil.rmtree(self._journal_path(study_id).parent)

            if study_id in self._files:
                self._files[study_id].close()
                del self._files[study_id]

            return _models.StudyModel(id=study_id, name=study_name)

    def get_all_studies(self) -> List[_models.StudyModel]:
        with self._file_lock(open(self._index_path(), "r"), readonly=True) as f:
            index = json.load(f)
            return [_models.StudyModel(id=id, name=name) for name, id in index["studies"].items()]

    def append_operations(self, ops: List[_models.OperationModel]) -> None:
        study_ops = {}
        for op in ops:
            if op.study_id not in study_ops:
                study_ops[op.study_id] = []

            study_ops[op.study_id].append(op)

        for study_id, ops in study_ops.items():
            with self._file_lock(self._get_journal_file(study_id), close=False) as f:
                f.seek(0, io.SEEK_END)
                for op in ops:
                    f.write(op.data)
                    f.write("\n")

    def read_operations(self, study_id: int, next_op_id: int) -> List[_models.OperationModel]:
        # Don't have to acquire lock here.
        f = self._get_journal_file(study_id)

        f.seek(next_op_id)
        ops = []
        while True:
            line = f.readline()
            if line[-1:] != "\n":
                break

            data = json.loads(line)
            ops.append(
                _models.OperationModel(id=f.tell() - 1, study_id=study_id, data=json.dumps(data))
            )

        return ops

    def _index_path(self):
        return self._root_dir.joinpath("index.json")

    def _journal_path(self, study_id: int):
        return self._root_dir.joinpath(str(study_id)).joinpath("journal.json")

    def _get_journal_file(self, study_id: int):
        if study_id not in self._files:
            self._files[study_id] = open(self._journal_path(study_id), "a+")

        return self._files[study_id]


class FcntlLock(object):
    def __init__(self, file, readonly: bool = False, close: bool = True) -> None:
        self._file = file
        self._readonly = readonly
        self._close = close

    def __enter__(self):
        if self._readonly:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_SH)
        else:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)

        return self._file

    def __exit__(self, ex_type, ex_value, trace):
        if not self._readonly:
            self._file.flush()

        fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        if self._close:
            self._file.close()


class LinkLock(object):
    def __init__(
        self, file, link_filename: str, readonly: bool = False, close: bool = True
    ) -> None:
        self._link_filepath = os.path.join(os.path.dirname(file.name), "lock/", link_filename)
        try:
            os.mkdir(os.path.dirname(self._link_filepath))
        except FileExistsError:
            pass

        self._file = file
        self._close = close
        self._readonly = readonly

    def _lock(self):
        wait_ms = 10
        while True:
            os.link(self._file.name, self._link_filepath)
            if os.stat(self._file.fileno()).st_nlink == 2:
                return
            os.unlink(self._link_filepath)
            time.sleep(random.uniform(0, wait_ms) / 1000)
            wait_ms = min(wait_ms * 2, 10 * 60 * 1000)

    def _unlock(self):
        os.unlink(self._link_filepath)

    def __enter__(self):
        self._lock()
        return self._file

    def __exit__(self, ex_type, ex_value, trace):
        if not self._readonly:
            self._file.flush()

        self._unlock()
        if self._close:
            self._file.close()


class LinkLockCreator(object):
    def __init__(self):
        self._id = str(uuid.uuid4())

    def __call__(self, file, readonly: bool = False, close: bool = True) -> LinkLock:
        return LinkLock(file, self._id, readonly, close)
