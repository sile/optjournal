from optjournal import JournalStorage, FileSystemDatabase
import optuna


db = FileSystemDatabase("optuna-db/")
storage = JournalStorage(db)

for s in optuna.get_all_study_summaries(storage):
    print(f"{s.study_name}: trials={s.n_trials}")
