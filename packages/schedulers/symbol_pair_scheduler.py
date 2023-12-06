import datetime
import heapq


class SymbolPairScheduler(object):
    def __init__(self):
        self.ready_symbol_pair_list = []
        self.waiting_symbol_pair_list = []
        self.error_symbol_pair_list = []

    def ready(self, symbol_pair: tuple[str, str]):
        self.ready_symbol_pair_list.append(symbol_pair)

    def extendReady(self, symbol_pair_list: list[tuple[str, str]]):
        self.ready_symbol_pair_list.extend(symbol_pair_list)

    def wait(self, symbol_pair: tuple[str, str], cooldown: datetime.timedelta):
        heapq.heappush(
            self.waiting_symbol_pair_list,
            (datetime.datetime.now() + cooldown, symbol_pair),
        )

    def error(self, symbol_pair: tuple[str, str]):
        self.error_symbol_pair_list.append(symbol_pair)

    def __update__(self):
        while len(self.waiting_symbol_pair_list) and (
            self.waiting_symbol_pair_list[0][0] <= datetime.datetime.now()
        ):
            symbol_pair = heapq.heappop(self.waiting_symbol_pair_list)[1]
            self.ready_symbol_pair_list.append(symbol_pair)

    def get(self, limit=1000) -> list[tuple[str, str]]:
        self.__update__()

        symbol_pair_list = self.ready_symbol_pair_list[:limit]
        self.ready_symbol_pair_list = self.ready_symbol_pair_list[limit:]

        return symbol_pair_list

    def readySize(self) -> int:
        self.__update__()
        return len(self.ready_symbol_pair_list)

    def waitingSize(self) -> int:
        self.__update__()
        return len(self.waiting_symbol_pair_list)

    def errorSize(self) -> int:
        return len(self.error_symbol_pair_list)
