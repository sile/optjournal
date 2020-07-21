MAX_TRIAL_NUM = 1000000


def get_study_id(trail_id: int) -> int:
    return trial_id // MAX_TRIAL_NUM
