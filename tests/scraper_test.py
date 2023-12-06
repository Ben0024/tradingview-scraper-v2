# import os
# import sys

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# import datetime
# import time

# from dotenv import load_dotenv

# from packages.tradingview.scraper.auth import getAuth
# from packages.schedulers.symbol_pair_scheduler import (
#     SymbolPairScheduler,
# )
# from packages.schedulers.task_scheduler import Task, TaskScheduler


# class TestClass:
#     def test_symbol_pair_scheduler(self):
#         s = SymbolPairScheduler()

#         pair = ("BINANCE:BTCUSDT", "1D")
#         s.ready(pair)
#         assert s.readySize() == 1
#         assert pair == s.get()[0]

#         s.wait(pair, datetime.timedelta(seconds=1))
#         assert s.readySize() == 0

#         time.sleep(1)
#         assert pair == s.get()[0]

#         s.extendReady([pair] * 10)
#         assert s.readySize() == 10
#         assert len(s.get()) == 10

#     def test_task_scheduler(self):
#         s = TaskScheduler()

#         task = Task("test", datetime.timedelta(seconds=1))

#         s.push(task, ready=True)
#         assert s.readySize() == 1
#         assert task.task_name == s.pop().task_name

#         s.push(task)
#         assert s.readySize() == 0

#         time.sleep(1)
#         assert task.task_name == s.pop().task_name

#     def test_auth(self):
#         load_dotenv(
#             dotenv_path=os.path.join(
#                 os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
#                 ".env.local",
#             )
#         )

#         tv_usernames = os.getenv("TV_USERNAME", "").split(",")
#         tv_passwords = os.getenv("TV_PASSWORD", "").split(",")
#         if len(tv_usernames) != len(tv_passwords):
#             raise Exception("Number of usernames and passwords are not equal")

#         pro_tv_usernames = os.getenv("PRO_TV_USERNAME", "").split(",")
#         pro_tv_passwords = os.getenv("PRO_TV_PASSWORD", "").split(",")
#         if len(pro_tv_usernames) != len(pro_tv_passwords):
#             raise Exception("Number of usernames and passwords are not equal")

#         cache_dir = os.path.join(
#             os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
#             "cache",
#         )

#         basic_auth = getAuth(tv_usernames[0], tv_passwords[0], cache_dir)
#         assert basic_auth is not None
#         assert basic_auth["auth_token"] is not None
#         assert basic_auth["is_pro"] is not None
#         assert basic_auth["pro_plan"] is not None

#         pro_auth = getAuth(pro_tv_usernames[0], pro_tv_passwords[0], cache_dir)
#         assert pro_auth is not None
#         assert pro_auth["auth_token"] is not None
#         assert pro_auth["is_pro"] is not None
#         assert pro_auth["pro_plan"] is not None
