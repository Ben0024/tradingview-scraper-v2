import datetime
import heapq


class Task(object):
    def __init__(
        self,
        task_name: str,
        task_interval: datetime.timedelta | None = None,  # maybe datetime.datetime
        task_repeat: bool = False,
    ):
        self.task_name = task_name
        self.task_interval = task_interval
        self.task_repeat = task_repeat


class TaskScheduler(object):
    def __init__(self):
        self.ready_task_list: list[Task] = []
        self.waiting_task_list: list[tuple[datetime.datetime, Task]] = []

    def push(self, task: Task, ready: bool = False):
        if ready:
            self.ready_task_list.append(task)
        else:
            assert (
                task.task_interval is not None
            ), "task_interval must be set if not pushing into ready queue"
            heapq.heappush(
                self.waiting_task_list,
                (datetime.datetime.now() + task.task_interval, task),
            )

    def __update__(self):
        while len(self.waiting_task_list) and (
            self.waiting_task_list[0][0] <= datetime.datetime.now()
        ):
            task = heapq.heappop(self.waiting_task_list)[1]
            self.ready_task_list.append(task)

    def pop(self) -> Task:
        self.__update__()

        if len(self.ready_task_list):
            return self.ready_task_list.pop()
        else:
            return heapq.heappop(self.waiting_task_list)[1]

    def readySize(self) -> int:
        self.__update__()

        return len(self.ready_task_list)
