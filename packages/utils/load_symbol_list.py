import json

import psycopg


def load_symbol_list(db_url: str, start: int = 0, limit: int = 10000) -> list[dict]:
    """load symbol list from database

    Returns:
        list[dict]: list of symbol_name, symbol_data
    """
    symbol_list = []
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol_id, symbol_name, symbol_data FROM symbol WHERE symbol_id >= %s ORDER BY symbol_id ASC LIMIT %s",
                (start, limit),
            )
            for row in cur:
                try:
                    symbol_list.append(
                        {
                            "symbol_id": row[0],
                            "symbol_name": row[1],
                            "symbol_data": json.loads(row[2]),
                        }
                    )
                except Exception:
                    continue
    return symbol_list
