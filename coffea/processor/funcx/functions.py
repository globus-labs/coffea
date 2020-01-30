import json

from funcx.sdk.client import FuncXClient

from coffea.processor.parsl.timeout import timeout


def process(item, stageout_url, timeout=None, **kwargs):
    import os
    import random
    import string
    import subprocess
    from collections.abc import Sequence

    import numpy as np

    from coffea.processor.executor import _work_function, _compression_wrapper, WorkItem
    from coffea.processor.accumulator import dict_accumulator
    from coffea.util import load, save

    # FIXME multiple processors will clobber eachother
    processor_path = os.path.join(os.environ['FUNCX_TMPDIR'], 'processor.coffea')
    # FIXME Commenting out for now so this can be cached;
    #       we need a way to clear the cache when appropriate
    # os.unlink(processor_path)
    if not os.path.isfile(processor_path):
        command = 'xrdcp {}/processor.coffea {}'.format(stageout_url, processor_path)
        res = subprocess.call(command, shell=True)

    outfile = ''.join(random.choice(string.ascii_lowercase) for i in range(10))
    subdir = str(np.random.choice(range(100)))
    if stageout_url.startswith('file://'):
        result_path = os.path.join(stageout_url.replace('file://', ''), subdir, outfile)
    elif stageout_url.startswith('root://'):
        result_path = os.path.join(os.environ['FUNCX_TMPDIR'], outfile)
    else:
        raise NotImplementedError('Only file and xrootd stageout supported')

    try:
        processor_instance = load(processor_path)
        compression = kwargs.pop('compression', 1)
        if compression is not None:
            _work_function = _compression_wrapper(compression, _work_function)
        output = _work_function(
            WorkItem(*item),
            processor_instance,
            **kwargs
        )
        save(output, result_path)
    except Exception as e:
        result_path += '.err'

        # # format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d [%(levelname)s]  %(message)s"
        # # logger = logging.getLogger(__name__)
        # # # logger.setLevel(logging.DEBUG)
        # # handler = logging.FileHandler(result_path)
        # # # handler.setLevel(logging.DEBUG)
        # # formatter = logging.Formatter(result_path, datefmt='%Y-%m-%d %H:%M:%S')
        # # handler.setFormatter(formatter)
        # # logger.addHandler(handler)

        # # hostname = subprocess.check_output('hostname', shell=True).strip().decode()
        # # logger.warn('problem loading processor instance for {} on {}'.format(str(item)), hostname)
        # # logger.warn('exception encountered: {}'.format(e))
        # # for key, value in os.environ.items():
        # #     logger.warn('[environment] {}: {}'.format(key, value))

        with open(result_path, 'w') as f:
            print('problem loading processor instance for {}'.format(str(item)), file=f)
            print(e, file=f)
            print('environment:', file=f)
            for key, value in os.environ.items():
                print('{}: {}'.format(key, value), file=f)
            print('hostname: {}'.format(subprocess.check_output('hostname', shell=True), file=f))


    if stageout_url.startswith('root://'):
        command = 'xrdcp {} {}'.format(result_path, os.path.join(stageout_url, subdir))
        subprocess.call(command, shell=True)
        os.unlink(result_path)

    return os.path.join(subdir, os.path.basename(result_path))

client = FuncXClient()
uuids = {}
for func in [process]:
    f = timeout(func)
    uuids[func.__name__] = client.register_function(f)

with open('data/function_uuids.json', 'w') as f:
    f.write(json.dumps(uuids, indent=4, sort_keys=True))
