import os
import subprocess

from coffea import hist, processor
from coffea.util import load
from copy import deepcopy
from concurrent.futures import Future
from collections.abc import Sequence
from functools import wraps
from itertools import cycle

from tqdm import tqdm
import cloudpickle as cpkl
import pickle as pkl
import lz4.frame as lz4f
import numpy as np
import time

from parsl.app.app import python_app
from ..executor import _futures_handler
from .detail import get_funcx_future

lz4_clevel = 1


class FuncXExecutor(object):

    def __init__(self):
        self._counts = {}
        self._processor_transferred = False

    def transfer_processor(self, processor, stageout_url):
        if self._processor_transferred is True:
            return
        command = 'xrdcp {} {}/processor'.format(processor, stageout_url)
        subprocess.call(command, shell=True)


    @property
    def counts(self):
        return self._counts

    def __call__(self, items, processor, output, endpoints, stageout_url,
            local_path=None, status=True, unit='items', desc='Processing',
            timeout=None, flatten=True, poll_period=0.1):

        self.transfer_processor(processor, stageout_url)

        futures = set()
        endpoint = cycle(endpoints)

        for dataset, fn, treename, chunksize, index in items:
            if dataset not in self._counts:
                self._counts[dataset] = 0
            payload = [dataset, fn, treename, chunksize, index, stageout_url]
            futures.add(get_funcx_future(payload, next(endpoint), 'process', poll_period, timeout=timeout, flatten=flatten))

        def accumulator(total, result):
            path = os.path.basename(result)
            if local_path is not None:
                path = os.path.join(local_path, path)
                blob, nevents, dataset = load(path)
            else:
                print('copying from stageout location')
                command = 'xrdcp {} .'.format(result)
                subprocess.call(command, shell=True)
                blob, nevents, dataset = load(path)
                os.unlink(path)

            total[1][dataset] += nevents
            total[0].add(pkl.loads(lz4f.decompress(blob)))

            # FIXME fails, perhaps auth error?
            # command = 'xrdfs rm {}'.format(result)
            # subprocess.call(command, shell=True)

        _futures_handler(futures, (output, self._counts), status, unit, desc, accumulator)


funcx_executor = FuncXExecutor()
