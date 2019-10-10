import sys
import os
import json
import time

from concurrent.futures import Future
import multiprocessing
from itertools import cycle

from tqdm import tqdm

from funcx.sdk.client import FuncXClient
from funcx.serialize import FuncXSerializer


from parsl.providers import LocalProvider
from parsl.channels import LocalChannel
from parsl.config import Config
from parsl.executors import HighThroughputExecutor

from ..executor import _futures_handler

client = FuncXClient()

try:
    from collections.abc import Sequence
except ImportError:
    from collections import Sequence

with open(os.path.join(os.path.dirname(__file__), 'function_uuids.json')) as f:
    function_uuids = json.load(f)


class TimeoutError(Exception):
    "Exception raised on timeout"


class FuncXFuture(Future):
    client = FuncXClient()
    serializer = FuncXSerializer()

    def __init__(self, task_id, poll_period=0.1):
        super().__init__()
        self.task_id = task_id
        self.poll_period = poll_period
        self.__result = None

    def done(self):
        if self.__result is not None:
            return True
        time.sleep(self.poll_period)  # needed to not overwhelm the FuncX server
        try:
            data = FuncXFuture.client.get_task_status(self.task_id)
        except Exception:
            return False
        if 'status' in data and data['status'] == 'PENDING':
            return False
        elif 'result' in data:
            self.__result = FuncXFuture.serializer.deserialize(data['result'])
            return True
        elif 'exception' in data:
            e = FuncXFuture.serializer.deserialize(data['exception'])
            e.reraise()
        else:
            raise NotImplementedError('task {} is neither pending or finished: {}'.format(self.task_id, str(data)))

    def result(self, timeout=None):
        if self.__result is not None:
            return self.__result
        while True:
            if self.done():
                break
            else:
                time.sleep(self.poll_period)
                if timeout is not None:
                    timeout -= self.poll_period
                    if timeout < 0:
                        raise TimeoutError

        return FuncXFuture.client.get_result(self.task_id)


def get_funcx_future(payload, endpoint, function, poll_period, retries=6, **kwargs):
    for attempt in range(retries):
        try:
            task_id = client.run(
                *payload,
                **kwargs,
                endpoint_id=endpoint,
                function_id=function_uuids[function]
            )
            break
        except Exception as e:
            print('encountered exception, will retry: {}'.format(str(e)))
            time.sleep(5)
    return FuncXFuture(task_id, poll_period)

def get_chunking(filelist, chunksize, endpoints, status=True, timeout=10, poll_period=0.1):
    futures = set()
    endpoint = cycle(endpoints)

    for ds, fn, tn in filelist:
        payload = [fn, tn, chunksize, ds, timeout]
        futures.add(get_funcx_future(payload, next(endpoint), 'derive_chunks', poll_period))

    items = []

    def accumulator(total, result):
        ds, treename, chunks = result
        for chunk in chunks:
            total.append((ds, chunk[0], treename, chunk[1], chunk[2]))

    _futures_handler(futures, items, status, 'files', 'Preprocessing', accumulator)

    return items
