from enum import Enum


class TaskType(str, Enum):
    LOAD_SYMBOL = "task_load_symbol"
    GET_BARS = "task_get_bars"
    UPDATE_LOGGER = "task_update_logger"
    UPDATE_AUTH = "task_update_auth"
