import optuna

import optjournal


def test_basic():
    storage = optjournal.JournalStorage("sqlite:///:memory:")
    study = optuna.create_study(study_name="foo", storage=storage)

    study.optimize(lambda t: t.suggest_float("x", 0, 1), n_trials=10)


def test_load_study():
    storage = optjournal.JournalStorage("sqlite:///:memory:")
    study = optuna.create_study(study_name="foo", storage=storage)
    study.optimize(lambda t: t.suggest_float("x", 0, 1), n_trials=10)

    study = optuna.load_study(study_name="foo", storage=storage)
    assert len(study.trials) == 10

    study = optuna.create_study(study_name="foo", storage=storage, load_if_exists=True)
    assert len(study.trials) == 10


def test_get_all_study_summaries():
    storage = optjournal.JournalStorage("sqlite:///:memory:")
    assert storage.get_all_study_summaries() == []

    study = optuna.create_study(study_name="foo", storage=storage)
    assert len(storage.get_all_study_summaries()) == 1

    summary = storage.get_all_study_summaries()[0]
    assert summary.study_name == "foo"
    assert summary.best_trial is None
