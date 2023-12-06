import json
from ..services.websockets import create_message


class ChartSessionData:
    def __init__(self, chart_idx: int, cs_id: str):
        self.chart_idx = chart_idx
        self.cs_id = cs_id
        self.series_idx = 0
        self.current_symbol_pair = None

        # received data pairs
        self.bars_list = []
        self.symbol_pair_list = []
        self.detail_list = []

    def _series_payload(self, idx, interval: str, max_bars: int):
        payload = [
            self.cs_id,
            f"sds_{self.chart_idx}",
            f"s{self.series_idx}",
            f"sds_sym_{idx}",
            interval,
            max_bars,
            "",
        ]

        # max_bars is not needed for modify_series
        if self.series_idx != 0:
            payload.pop(-2)
        return payload

    def _resolve_payload(self, idx, symbol: str):
        content = json.dumps(
            {
                "symbol": symbol,
                "adjustment": "splits",
                "session": "extended",
            }
        )
        return [
            self.cs_id,
            f"sds_sym_{idx}",
            "=" + content,
        ]

    async def send_request(
        self, ws, idx, symbol_pair: tuple[str, str], max_bars: int = 50000
    ):
        self.current_symbol_pair = symbol_pair
        symbol, interval = symbol_pair

        # resolve symbol
        await ws.send(
            create_message("resolve_symbol", self._resolve_payload(idx, symbol))
        )
        func_name = "create_series" if self.series_idx == 0 else "modify_series"
        await ws.send(
            create_message(func_name, self._series_payload(idx, interval, max_bars))
        )
        self.series_idx += 1
