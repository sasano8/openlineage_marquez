from queue import Queue
from dataclasses import dataclass
import queue
import threading
from copy import deepcopy
from typing import Callable

from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset

from openlineage.client.facet_v2 import (
    sql_job,
    source_code_job,
    source_code_location_job,
    error_message_run,
)


from datetime import datetime, timezone
from uuid import uuid4


class JobEventBuilder:
    """来歴の RunEvent を構築を支援する。
    RunEvent の構築が大変なため、必要最小限

    Usage:

    """

    @classmethod
    def from_activity(cls) -> "JobEventBuilder":
        """activity から RunEvent を生成する"""
        raise NotImplementedError()

    @classmethod
    def from_kwargs(
        cls, namespace: str, name: str, run_id: str = None
    ) -> "JobEventBuilder":
        """引数から RunEvent を生成する"""
        event = RunEvent(
            eventType=RunState.START,
            eventTime=datetime.now(timezone.utc).isoformat(),
            job=Job(namespace=namespace, name=name, facets={}),
            run=Run(run_id or str(uuid4())),
            inputs=[],
            outputs=[],
            producer="OpenLineage.io/website/blog",
            schemaURL="https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
        )
        return cls(event)

    def __init__(self, event: RunEvent):
        self._event = event

    @property
    def namespace(self):
        return self._event.job.namespace

    @property
    def name(self):
        return self._event.job.name

    @property
    def run_id(self):
        return self._event.run.runId

    @property
    def parent(self) -> RunEvent:
        parent = self._event.job.facets.get("parent", None)
        assert isinstance(parent, RunEvent)
        return parent

    def attach_facets(
        self,
        parent: RunEvent = None,
        params: dict = None,
        sql: str = None,
        error: str = None,
    ):
        """
        追加の情報を設定する。

        Usage:
            job.attach_facets(
                parent=parent_run_event,
                params={"param1": 1},
                sql="select * from yourtable",
                error="An error occured."
            )
        """
        if parent:
            self._event.job.facets["parent"] = parent

        if error:
            self._event.run.facets["errorMessage"] = (
                error_message_run.ErrorMessageRunFacet(
                    message=error, programmingLanguage=""
                )
            )

        if sql:
            self._event.job.facets["sql"] = sql_job.SQLJobFacet(query=sql)

        if params:
            # 実行パラムを載せたい
            self._event.job.facets["params"] = params

        # ソースプログラムの情報を載せたい
        # "source_code": source_code_job.SourceCodeJobFacet("python", "xxx.py"),
        # "source_code_location": source_code_location_job.SourceCodeLocationJobFacet(type="git", url="", repoUrl="https://github.com/exapmele/myapp.git", branch="develop"),

        return self

    def update_run_id(self, run_id=None):
        run_id = run_id or str(uuid4())
        self._event.run = Run(run_id)
        return self

    def update_event_time(self):
        self._event.eventTime = datetime.now(timezone.utc).isoformat()
        return self

    def update_event_type(self, event_type: RunState):
        self._event.eventType = event_type
        return self

    def attach_input(self, name: str, facets: dict = {}, namespace: str = None):
        namespace = namespace or self.namespace
        self._event.inputs.append(
            Dataset(namespace=namespace, name=name, facets=facets)
        )
        return self

    def attach_output(self, name: str, facets: dict = {}, namespace: str = None):
        namespace = namespace or self.namespace
        self._event.outputs.append(
            Dataset(namespace=namespace, name=name, facets=facets)
        )
        return self

    def complete(self, error: str = ""):
        """実行を完了する。error を指定した場合は、FAIL 扱いとなる。"""
        self.update_event_type(RunState.FAIL if error else RunState.COMPLETE)
        self.attach_facets(error=error)
        return self

    def dump(self):
        """event_time を現在日時に更新し、データを取得する"""
        return self.get_event(True)

    def get_event(self, update_event_time: bool = False):
        if update_event_time:
            self.update_event_time()
        return deepcopy(self._event)


class MessageQueue:
    """スレッド間でデータをやり取りする。
    スレッド、キュー、イベント（キャンセルトークン）を管理する。
    """

    @classmethod
    def create_and_start(cls, _func, **kwargs):
        mq = cls.create(_func, **kwargs)
        mq.start()
        return mq

    @classmethod
    def create(cls, _func, **kwargs):
        """
        func: 実行する関数
        **kwargs: 関数に渡す引数（第１引数 queue、第２引数 cancel_token は予約されている）
        """
        from functools import partial

        q = Queue()
        e = threading.Event()

        kwargs = dict(queue=q, cancel_token=e, **kwargs)
        f = partial(_func, **kwargs)

        return cls(f, q, e)

    def __init__(self, func, queue, event):
        self._func = func
        self._queue = queue
        self._event = event
        self._thread = None

    def start(self):
        from functools import partial

        if self._thread:
            raise RuntimeError()

        if isinstance(self._func, partial):
            target = self._func.func
            args = self._func.args
            kwargs = self._func.keywords
        else:
            target = self._func
            args = tuple()
            kwargs = {}

        self._thread = threading.Thread(target=target, args=args, kwargs=kwargs)
        self._thread.start()

    def stop(self):
        self._event.set()
        self._thread = None

    def join(self):
        self._event.set()
        self._thread.join()
        # self._queue.join()
        self._thread = None

    def emit(self, obj):
        self._queue.put(obj)


def consume_lineage_event(queue: Queue, cancel_token: threading.Event, client_factory):
    """キューを介して、openlineage バックエンドでデータを送信する"""
    import time

    client = client_factory()

    while not queue.empty():
        try:
            while not queue.empty():
                event = queue.get()
                client.emit(event)
        except Exception as e:
            print(e)  # TODO: print でなく logger に

        if cancel_token.is_set():
            return

        time.sleep(1)
