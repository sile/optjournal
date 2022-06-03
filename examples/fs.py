from optjournal import JournalStorage, FileSystemDatabase
import optuna


def objective(trial):
    x = trial.suggest_float('x', 0, 1)
    y = trial.suggest_float('y', 0, 1)
    return x * y


db = FileSystemDatabase("optuna-db/")
storage = JournalStorage(db)
study = optuna.create_study(storage=storage)
study.optimize(objective, n_trials=100)
