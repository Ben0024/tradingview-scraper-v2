import datetime
import json
import logging
import multiprocessing
import os
import time
import traceback
from datetime import timedelta
from typing import Literal
from ..types.task import TaskType
from ..services.auth import get_auth
from ..utils.load_symbol_list import load_symbol_list
from ..scrapers.tradingview import get_multiple_bars
from ..utils.intervals import get_interval_list, interval_to_second
from ..schedulers.symbol_pair_scheduler import SymbolPairScheduler
from ..schedulers.task_scheduler import TaskScheduler, Task
from ..services.write import (
    get_last_line_ts,
    get_symbol_pair_filepath,
    write_empty_file,
    write_to_file,
)
from ..constants.intervals import MIN_INTERVAL_BARS, MAX_INTERVAL


class ScraperEngine:
    def __init__(
        self,
        engine_name: str,
        username: str,
        password: str,
        log_dir: str,
        log_formatter: logging.Formatter,
        storage_dir: str,
        cache_dir: str,
        db_url: str,
        limit_per_load: int = 1000,
        num_process: int = 1,
        max_cs: int = 10,
    ):
        # basic settings
        self.engine_name = engine_name
        self.username = username
        self.password = password
        self.log_dir = log_dir
        self.log_formatter = log_formatter
        self.storage_dir = storage_dir
        self.db_url = db_url
        self.limit_per_load = limit_per_load
        self.num_processes = num_process
        self.max_cs = max_cs
        self.cache_dir = cache_dir

        # logger
        self.logger = logging.getLogger(self.engine_name)
        self.logger.setLevel(logging.INFO)
        self.updateLogger()

        # auth
        self.auth = None
        self.auth_token = None
        self.updateAuth()

        # symbol
        self.next_symbol_id = 0
        self.symbol_data = {}
        self.task_scheduler = TaskScheduler()
        self.symbol_pair_scheduler = SymbolPairScheduler()

        self.start_time = time.time()
        self.pair_set = set()

    def _create_log_handler(
        self, log_type: Literal["info", "err"]
    ) -> logging.FileHandler:
        filepath = os.path.join(
            self.log_dir,
            "scraper-{}-{}.{}.log".format(
                time.strftime("%Y%m%d"), self.engine_name, log_type
            ),
        )
        log_file_handler = logging.FileHandler(filepath, encoding="utf-8")
        log_file_handler.setLevel(logging.INFO)
        log_file_handler.setFormatter(self.log_formatter)
        return log_file_handler

    def updateLogger(self):
        for handler in self.logger.handlers:
            self.logger.removeHandler(handler)

        info_log_handler = self._create_log_handler("info")
        err_file_handler = self._create_log_handler("err")
        self.logger.addHandler(info_log_handler)
        self.logger.addHandler(err_file_handler)

    def updateAuth(self):
        """
        update auth token
        """
        self.logger.info("Getting auth token...")
        self.auth = get_auth(
            username=self.username,
            password=self.password,
            cache_dir=self.cache_dir,
        )
        if self.auth is None:
            self.logger.error("Failed to get auth")
        else:
            auth_token = self.auth["auth_token"]
            is_pro = self.auth["is_pro"]
            self.auth_token = auth_token
            self.logger.info(f"Got auth token: {auth_token}, is pro: {is_pro}")

    def load_symbol_list(self) -> list[str]:
        self.logger.info("Loading symbol list...")
        raw_symbol_list = load_symbol_list(
            self.db_url, self.next_symbol_id, self.limit_per_load
        )
        self.logger.info(
            f"{len(raw_symbol_list)} symbols loaded that starts from id: {self.next_symbol_id}"
        )

        symbol_list = [symbol["symbol_name"] for symbol in raw_symbol_list]
        if len(raw_symbol_list) > 0:
            self.next_symbol_id = raw_symbol_list[-1]["symbol_id"] + 1
            self.symbol_data.update(
                {
                    symbol["symbol_name"]: symbol["symbol_data"]
                    for symbol in raw_symbol_list
                }
            )
        return symbol_list

    def _schedule_symbol_pair(self, symbol: str, interval: str):
        """
        Determine whether to this symbol pair is ready to crawl or not
        Ready: add it to the ready queue.
        Wait:  add it to the waiting queue.
        """
        filepath = get_symbol_pair_filepath(self.storage_dir, symbol, interval)
        symbol_pair = (symbol, interval)
        period = interval_to_second(interval) * MIN_INTERVAL_BARS
        delta = datetime.timedelta(seconds=min(period, MAX_INTERVAL))

        if os.path.exists(filepath):
            last_timestamp = get_last_line_ts(filepath)

            if last_timestamp is not None and last_timestamp <= time.time() - period:
                # Last crawled too old
                self.symbol_pair_scheduler.ready(symbol_pair)
            else:
                # No data in last crawling, or last crawling is too recent
                self.symbol_pair_scheduler.wait(symbol_pair, delta)
        else:
            # If file not exists, assume that we haven't crawled this pair
            self.symbol_pair_scheduler.ready(symbol_pair)

    def load_symbol_pair_list(self):
        symbol_list = self.load_symbol_list()
        for symbol in symbol_list:
            interval_list = []
            is_pro = self.auth is not None and self.auth["is_pro"]
            if self.symbol_data[symbol]["type"] == "economic":
                # economic data is only available for pro users
                if not is_pro:
                    continue
                interval_list = get_interval_list("months")
            else:
                # seconds interval is only available for pro users
                select = "seconds" if is_pro else "non seconds"
                interval_list = get_interval_list(select)

            # schedule symbol pair
            for interval in interval_list:
                self._schedule_symbol_pair(symbol, interval)

    def getBars(self):
        # get scheduled symbol pair list
        symbol_pair_list = self.symbol_pair_scheduler.get(1000)
        ready_size = self.symbol_pair_scheduler.readySize()
        waiting_size = self.symbol_pair_scheduler.waitingSize()
        error_size = self.symbol_pair_scheduler.errorSize()

        # if no scheduled symbol pair, return
        if len(symbol_pair_list) == 0:
            logging.warning("No symbol pair to crawl")
            return

        self.logger.info(
            "Getting bars for {} pairs, ready: {}, waiting: {}, error: {}".format(
                len(symbol_pair_list), ready_size, waiting_size, error_size
            )
        )

        # get bars for each pair in symbol pair list
        results = get_multiple_bars(
            logger=self.logger,
            auth_token=self.auth_token or "",
            symbol_pair_list=symbol_pair_list,
            num_processes=self.num_processes,
            max_cs=self.max_cs,
        )

        symbol_pair_set = set(symbol_pair_list)

        # iterate each pair in results
        for i, result in enumerate(results):
            symbol, interval, bars, _ = result
            if (symbol, interval) in symbol_pair_set:
                symbol_pair_set.remove((symbol, interval))
            else:
                assert False, "Got duplicate result or result not in requested"

            if bars is None or len(bars) == 0:
                # bars is empty, create empty file
                message = f"Got no bars for pair ({symbol},{interval})"
                write_empty_file(self.storage_dir, symbol, interval)
                self.logger.warning(f"{i+1}/{len(results)} {message}")
            elif "v" not in bars[0] or len(bars[0]["v"]) > 6:
                # Bars are invalid
                # > 6 means that the bars are not in the form of [timestamp, open, high, low, close, volume]
                message = (
                    f"Not supported pair ({symbol},{interval})\nbars[0]: {bars[0]}"
                )
                self.logger.warning(f"{i+1}/{len(results)} {message}")
            else:
                # bars are valid write bars to file
                message = "Got {:6d} bars for pair: ({:>20s},{:>5s}), range: ({:d}, {:d})".format(
                    len(bars),
                    symbol,
                    interval,
                    int(bars[0]["v"][0]),
                    int(bars[-1]["v"][0]),
                )
                res = write_to_file(self.storage_dir, symbol, interval, bars)
                self.logger.info(f"{i+1}/{len(results)} {message}")
                if res["status"] != "ok":
                    self.logger.error(f"Write file: {json.dumps(res, indent=4)}")

            self.pair_set.add((symbol, interval))
            if bars is None or len(bars) == 0:
                self.symbol_pair_scheduler.error((symbol, interval))
            else:
                period = interval_to_second(interval) * MIN_INTERVAL_BARS
                delta = datetime.timedelta(seconds=min(period, MAX_INTERVAL))
                self.symbol_pair_scheduler.wait((symbol, interval), delta)

        duration = time.time() - self.start_time
        num_pairs = len(self.pair_set)
        self.logger.info(
            f"Duration {duration}, {num_pairs} pairs, average {num_pairs/duration} pairs per sec"
        )

        if len(symbol_pair_set) > 0:
            self.logger.warning(
                f"Got {len(results)} results, expected {len(symbol_pair_list)}"
            )
            self.symbol_pair_scheduler.extendReady(list(symbol_pair_set))

    def handleTask(self, task: Task):
        task_name = task.task_name
        self.logger.info(f"Handling task: {task_name}")
        match task_name:
            case TaskType.UPDATE_AUTH:
                self.updateAuth()
            case TaskType.LOAD_SYMBOL:
                self.load_symbol_pair_list()
                if self.symbol_pair_scheduler.readySize() > 0:
                    self.task_scheduler.push(Task("task_get_bars"), ready=True)
            case TaskType.GET_BARS:
                self.getBars()
                if self.symbol_pair_scheduler.readySize() > 0:
                    self.task_scheduler.push(Task("task_get_bars"), ready=True)
            case TaskType.UPDATE_LOGGER:
                self.updateLogger()
        if task.task_repeat:
            self.task_scheduler.push(task, ready=False)

    def start(self):
        current_process = multiprocessing.current_process()

        self.task_scheduler.push(
            Task(TaskType.LOAD_SYMBOL.value, timedelta(minutes=1), True),
            ready=True,
        )
        self.task_scheduler.push(
            Task(TaskType.GET_BARS.value, timedelta(minutes=1), True),
            ready=True,
        )
        self.task_scheduler.push(
            Task(TaskType.UPDATE_LOGGER.value, timedelta(days=1), True),
            ready=False,
        )
        self.task_scheduler.push(
            Task(TaskType.UPDATE_AUTH.value, timedelta(hours=1), True),
            ready=False,
        )

        while current_process.exitcode is None:
            if self.task_scheduler.readySize() > 0:
                task = self.task_scheduler.pop()
            else:
                time.sleep(1)
                continue

            try:
                self.handleTask(task)
            except Exception as e:
                self.logger.error(f"Exception: {e}")
                self.logger.error(f"Traceback: {traceback.format_exc()}")
