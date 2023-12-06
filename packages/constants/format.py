"""
m: "qsd"
p: [
    QUOTE_SESSION_ID,
    {
        "n": SYMBOL_STRING,
        "s": STATUS,
        "v": OBJECTS,
    }
]

m: "quote_completed"
p: [
    QUOTE_SESSION_ID,
    SYMBOL_STRING
]

m: "series_loading"
p: [
    CHART_SESSION_ID,
    SERIES_ID_A,
    SERIES_ID_B,
]

m: "symbol_resolved"
p: [
    CHART_SESSION_ID,
    SERIES_SYMBOL_ID,
    OBJECT
]

m: "timescale_update"
p: [
    CHART_SESSION_ID,
    {
        SERIES_ID_A: {
            "node": NODE_NAME,
            "s": [
                {
                    "i": 0,
                    "v": [START_TIMESTAMP_OF_BAR, OPEN, HIGH, LOW, CLOSE, VOLUME],
                },
                ...
            ],
            "ns": {
                "d": STRING,
                "indexes": []
            },
            "t": SERIES_ID,
            "lbs": {"bar_close_time": STOP_TIMESTAMP_OF_LAST_BAR},
        }
    },
    {
        "index" : INDEX,
        "zoffset" : ZOFFSET,
        "changes" : [
            TIMESTAMP,
            TIMESTAMP,
            ...
        ]
        "marks" : [
            [?, TIMESTAMP, INDEX],
            [?, TIMESTAMP, INDEX],
            ...
        ]
        "index_diff" : INDEX_DIFF,
    }
]

m: "series_completed"
p: [
    CHART_SESSION_ID,
    SERIES_ID_A,
    "streaming",
    SERIES_ID_B,
    {
        "rt_update_period" : RT_UPDATE_PERIOD,
    }
]

m: "du"
p: [
    CHART_SESSION_ID,
    {
        SERIES_ID_A: Object
    }
]

"m": "series_timeframe"
"p": ["cs_4jaQgLNAlKut", "sds_1", "s1", 357, 1446, "LASTSESSION", true]

"m": "study_loading"
"p": ["cs_4jaQgLNAlKut", "st1", "s1_st1"]

"m": "study_completed"
"p": ["cs_4jaQgLNAlKut", "st1", "s1_st1"]

"m": "critical_error"

"m": "protocol_error"

"m": "series_error"
"""
