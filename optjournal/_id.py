# TODO: rename mdoule.

MAX_TRIAL_NUM = 1000000


def get_study_id(trial_id: int) -> int:
    return trial_id // MAX_TRIAL_NUM


def get_trial_number(trial_id: int) -> int:
    return trial_id % MAX_TRIAL_NUM
