optjournal
==========

Yet another Optuna RDB storage using journaling technique.

Installation
------------

```
pip install --user git+https://github.com/sile/optjournal.git
```

Example
-------

```python
from optjournal import JournalStorage
import optuna


def objective(trial):
    x = trial.suggest_float('x', 0, 1)
    y = trial.suggest_float('y', 0, 1)
    return x * y


storage = JournalStorage("sqlite:///optuna.db")
study = optuna.create_study(storage=storage)
study.optimize(objective, n_trials=100)
```
