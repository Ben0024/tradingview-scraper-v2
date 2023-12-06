SECONDS_LIST = ["1S", "5S", "10S", "15S", "30S"]
MINUTES_LIST = ["1", "3", "5", "15", "30", "60", "120", "240"]
DAYS_LIST = ["1D", "1W"]
MONTHS_LIST = ["1M", "3M", "6M", "12M"]
NON_SECONDS_LIST = MINUTES_LIST + DAYS_LIST + MONTHS_LIST

INTERVAL_LIST_DICT = {
    "seconds": SECONDS_LIST,
    "minutes": MINUTES_LIST,
    "days": DAYS_LIST,
    "months": MONTHS_LIST,
    "non seconds": NON_SECONDS_LIST,
    "all": SECONDS_LIST + NON_SECONDS_LIST,
}

MIN_INTERVAL_BARS = int(2000)
MAX_INTERVAL = int(60 * 60 * 24 * 7)
