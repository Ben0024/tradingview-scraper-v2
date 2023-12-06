from typing import Literal
from ..constants.intervals import INTERVAL_LIST_DICT


def get_interval_list(
    select: Literal["seconds", "minutes", "days", "months", "non seconds", "all"]
) -> list[str]:
    """Get list of intervals

    Args:
        select (Literal["second", "non second", "all"]): which intervals to select
        symbol_data (dict, optional): symbol data. Defaults to None.

    Returns:
        list: list of intervals
    """
    candidate_list = []

    if select in INTERVAL_LIST_DICT:
        candidate_list = INTERVAL_LIST_DICT[select]
    else:
        assert False, "Invalid select"

    return candidate_list


def interval_to_second(interval: str) -> int:
    """Convert interval to seconds

    Args:
        interval (str): interval

    Returns:
        int: seconds
    """
    unit = interval[-1]
    val = int(interval[:-1] if interval[:-1] != "" else 1)
    try:
        match unit:
            case "S":
                val = val
            case "D":
                val = val * 24 * 60 * 60
            case "W":
                val = val * 7 * 24 * 60 * 60
            case "M":
                val = val * 30 * 24 * 60 * 60
            case _:
                val = val * 60
    except Exception:
        assert False, f"Invalid interval: {interval}"
    else:
        return val


def cmp_interval(t1: str, t2: str) -> int:
    """Compare interval

    Args:
        t1 (str): interval 1
        t2 (str): interval 2

    Returns:
        int: -1 if t1 < t2, 0 if t1 == t2, 1 if t1 > t2
    """
    return interval_to_second(t1) - interval_to_second(t2)
