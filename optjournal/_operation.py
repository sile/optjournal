import enum


class _Operation(enum.Enum):
    CREATE_STUDY = 0
    CREATE_TRIAL = 1
    SET_STUDY_USER_ATTR = 2
    SET_STUDY_SYSTEM_ATTR = 3
    SET_STUDY_DIRECTION = 4
    SET_TRIAL_PARAM = 5
