import optuna

import optjournal


def test_basic():
    storage = optjournal.RDBJournalStorage("sqlite:///:memory:")
    study = optuna.create_study(study_name="foo", storage=storage)

    study.optimize(lambda t: t.suggest_float("x", 0, 1), n_trials=10)


def test_load_study():
    storage = optjournal.RDBJournalStorage("sqlite:///:memory:")
    study = optuna.create_study(study_name="foo", storage=storage)
    study.optimize(lambda t: t.suggest_float("x", 0, 1), n_trials=10)

    study = optuna.load_study(study_name="foo", storage=storage)
    assert len(study.trials) == 10

    study = optuna.create_study(study_name="foo", storage=storage, load_if_exists=True)
    assert len(study.trials) == 10
