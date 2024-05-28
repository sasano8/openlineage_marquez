from threading import Thread
import requests
from copy import deepcopy
from openlineage.client import OpenLineageClient
from openlineage.client.transport.http import (
    ApiKeyTokenProvider,
    HttpConfig,
    HttpCompression,
    HttpTransport,
)

from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset

from openlineage.client.facet_v2 import (
    sql_job,
    source_code_job,
    source_code_location_job,
    error_message_run,
)


from openlineage.client import OpenLineageClient
from datetime import datetime, timezone
from uuid import uuid4

from helper import JobEventBuilder, MessageQueue, consume_lineage_event

import pytest


def get_client():
    http_config = HttpConfig(
        url="https://backend:5000",
        endpoint="api/v1/lineage",
        timeout=5,
        verify=False,
        auth=ApiKeyTokenProvider({"apiKey": "f048521b-dfe8-47cd-9c65-0cb07d57591e"}),
        compression=HttpCompression.GZIP,
    )

    client = OpenLineageClient(transport=HttpTransport(http_config))
    return client


# https://openlineage.io/docs/client/python


def get_local_client():
    return OpenLineageClient(url="http://localhost:5000")


@pytest.fixture
def ol():
    return get_local_client()


def test_scenario_1(ol: OpenLineageClient):
    """生データでRunEventを構築"""
    start = {
        "eventType": "START",
        "eventTime": "2020-12-28T19:52:00.001+10:00",
        "run": {"runId": "d46e465b-d358-4d32-83d4-df660ff614dd"},
        "job": {"namespace": "my-namespace", "name": "my-job"},
        "inputs": [{"namespace": "my-namespace", "name": "my-input"}],
        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
    }

    end = {
        "eventType": "COMPLETE",
        "eventTime": "2020-12-28T20:52:00.001+10:00",
        "run": {"runId": "d46e465b-d358-4d32-83d4-df660ff614dd"},
        "job": {"namespace": "my-namespace", "name": "my-job"},
        "outputs": [
            {
                "namespace": "my-namespace",
                "name": "my-output",
                "facets": {
                    "schema": {
                        "_producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
                        "_schemaURL": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/spec/OpenLineage.json#/definitions/SchemaDatasetFacet",
                        "fields": [
                            {"name": "a", "type": "VARCHAR"},
                            {"name": "b", "type": "VARCHAR"},
                        ],
                    }
                },
            }
        ],
        "producer": "https://github.com/OpenLineage/OpenLineage/blob/v1-0-0/client",
        "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json#/definitions/RunEvent",
    }

    # http://localhost:5000/api/v1/lineage
    requests.post("http://localhost:5000/api/v1/lineage", json=start).raise_for_status()
    requests.post("http://localhost:5000/api/v1/lineage", json=end).raise_for_status()


def test_scenario_2(ol):
    """クライアントスキーマを使ってRunEventを構築"""

    producer = "OpenLineage.io/website/blog"
    job = Job(
        namespace="food_delivery",
        name="example.order_data2",
        facets={
            # "sql": sql_job.SQLJobFacet(query="select 1 from xxx;"),
            # "source_code": source_code_job.SourceCodeJobFacet("python", "xxx.py"),
            # "source_code_location": source_code_location_job.SourceCodeLocationJobFacet(type="git", url="", repoUrl="https://github.com/exapmele/myapp.git", branch="develop"),
            "error_message_run": error_message_run.ErrorMessageRunFacet(
                message="error!!!", programmingLanguage=""
            )
        },
    )
    run = Run(str(uuid4()))

    ds1 = Dataset(namespace="food_delivery", name="public.input_1")
    ds2 = Dataset(namespace="food_delivery", name="public.input_2")

    ds3 = Dataset(namespace="food_delivery", name="public.output_1")
    ds4 = Dataset(namespace="food_delivery", name="public.output_2")

    ol.emit(
        RunEvent(
            RunState.COMPLETE,
            datetime.now().isoformat(),
            run,
            job,
            producer,
            inputs=[ds1, ds2],
            outputs=[ds3, ds4],
        )
    )


def test_scenario_3(ol):
    """RunEventの構築が大変なので、ヘルパーを用意してみる"""

    class IncrementStr:
        def __init__(self, start: int):
            self.value = start - 1

        def __radd__(self, a):
            self.value += 1
            return a + str(self.value)

    # mq = ol
    mq = MessageQueue.create_and_start(
        consume_lineage_event, client_factory=get_local_client
    )

    serial = IncrementStr(120)

    job = JobEventBuilder.from_kwargs(namespace="myspace", name="myfirstjob_" + serial)

    # ジョブ１開始イベント
    job.attach_input(name="public.input_" + serial)
    job.attach_input(name="public.input_" + serial)
    job.attach_facets(params={"param1": "a", "param2": "b"})
    job.attach_facets(sql="select 1 from xxx;")
    mq.emit(job.dump())

    # ジョブ２完了イベント
    job.attach_output(name="public.output_a")
    job.attach_output(name="public.output_b")
    job.complete()
    mq.emit(job.dump())

    # ジョブ２開始イベント
    parent = job.get_event()
    job = JobEventBuilder.from_kwargs(namespace="myspace", name="myfirstjob_" + serial)
    job.attach_input(name="public.output_a")
    job.attach_input(name="public.output_b")
    job.attach_facets(parent=parent)
    job.attach_facets(params={"param1": "a", "param2": "b"})
    job.attach_facets(sql="select 1 from xxx;")
    mq.emit(job.dump())

    # ジョブ２失敗イベント
    job.attach_output(name="public.output_" + serial)
    job.attach_output(namespace="aaaa", name="public.output_" + serial)
    job.complete(error="error!!")
    mq.emit(job.dump())
