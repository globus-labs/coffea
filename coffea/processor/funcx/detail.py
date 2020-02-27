import sys
import os
import json
import time
import sqlite3

from concurrent.futures import Future
import multiprocessing
from itertools import cycle

from tqdm.auto import tqdm

from globus_sdk.exc import GlobusAPIError

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

with open(os.path.join(os.path.dirname(__file__), 'data', 'function_uuids.json')) as f:
    function_uuids = json.load(f)


class TimeoutError(Exception):
    "Exception raised on timeout"


class FuncXFuture(Future):
    client = FuncXClient()
    serializer = FuncXSerializer()

    def __init__(self, task_id, poll_period=1):
        super().__init__()
        self.task_id = task_id
        self.poll_period = poll_period
        self.__result = None
        self.submitted = time.time()

    def done(self):
        if self.__result is not None:
            return True
        try:
            data = FuncXFuture.client.get_task_status(self.task_id)
        except Exception:
            return False
        if 'status' in data and data['status'] == 'PENDING':
            time.sleep(self.poll_period)  # needed to not overwhelm the FuncX server
            return False
        elif 'result' in data:
            self.__result = FuncXFuture.serializer.deserialize(data['result'])
            self.returned = time.time()
            # FIXME AW benchmarking
            self.connected_managers = os.environ.get('connected_managers', -1)

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

        return self.__result

class MappedFuncXFuture(Future):
    client = FuncXClient(funcx_service_address='https://dev.funcx.org/api/v1')

    def __init__(self, task_ids, accumulate=None, poll_period=2, progress_bar=True, retries=6):
        super().__init__()
        self.pending_task_ids = task_ids
        self.completed_task_ids = []
        self.poll_period = poll_period
        self.__result = None
        self.submitted = time.time()
        self.status = None
        self.progress_bar = progress_bar
        self.retries = retries

        if accumulate is None:
            def accumulate(accumulated_result, result):
                if accumulated_result is None:
                    return [result]
                return accumulated_result + [result]

        self.accumulate = accumulate

    def done(self):
        if len(self.pending_task_ids) == 0:
            return True

        for attempt in range(self.retries):
            try:
                self.status = MappedFuncXFuture.client.get_batch_status(self.pending_task_ids)
                break
            except GlobusAPIError:
                print('Encountered GlobusAPIError-- will retry ({} attempts remaining)'.format(self.retries - attempt - 1)
                time.sleep(self.poll_period)
        completed = [t for t in self.status.keys() if t in self.status and not self.status[t]['pending']]
        if len(completed) == len(self.pending_task_ids):
            return True
        return False

    def result(self, timeout=None):
        start = time.time()
        if self.progress_bar:
            pbar = tqdm(unit='task', total=len(self.pending_task_ids + self.completed_task_ids))

        while True:
            if self.done():
                break
            else:
                for task_id, data in self.status.items():
                    if self.progress_bar:
                        pbar.update(1)
                    self.pending_task_ids.remove(task_id)
                    self.completed_task_ids += [task_id]
                    self.__result = self.accumulate(self.__result, data['result'])

                time.sleep(self.poll_period)
                if timeout is not None:
                    time_elapsed = time.time() - start
                    if time_elapsed > timeout:
                        raise TimeoutError

        if len(self.pending_task_ids) > 0:
            for task_id, data in self.status.items():
                if self.progress_bar:
                    pbar.update(1)
                self.pending_task_ids.remove(task_id)
                self.completed_task_ids += [task_id]
                self.__result = self.accumulate(self.__result, data['result'])

        return self.__result


def get_funcx_future(payload, endpoint, function, poll_period, **kwargs):
    task_id = client.run(
        *payload,
        **kwargs,
        endpoint_id=endpoint,
        function_id=function_uuids[function]
    )
    return FuncXFuture(task_id, poll_period)

# def get_funcx_future(payload, endpoint, function, poll_period, retries=6, **kwargs):
#     for attempt in range(retries):
#         try:
#             task_id = client.run(
#                 *payload,
#                 **kwargs,
#                 endpoint_id=endpoint,
#                 function_id=function_uuids[function]
#             )
#             break
#         except Exception as e:
#             print('encountered exception, will retry: {}'.format(str(e)))
#             time.sleep(10)
#     return FuncXFuture(task_id, function, poll_period)

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

    _futures_handler(futures, items, status, 'files', 'Preprocessing', accumulator, timeout)

    return items
