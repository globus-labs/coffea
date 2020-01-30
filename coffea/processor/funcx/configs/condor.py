import os

from funcx.config import Config
from funcx.strategies import SimpleStrategy
from parsl.providers import CondorProvider
from parsl.executors import HighThroughputExecutor
import coffea.processor.funcx

proxy = '/tmp/x509up_u{}'.format(os.getuid())
if not os.path.isfile(proxy):
    raise RuntimeError('No valid proxy found-- run `voms-proxy-init -voms cms`')

# TODO: automate this-- see lobster/lobster/core/source.py and lobster/lobster/util.py
# parrot_bin = '/afs/crc.nd.edu/user/a/awoodard/coffeandbacon/analysis/bin'
# parrot_lib = '/afs/crc.nd.edu/user/a/awoodard/coffeandbacon/analysis/lib'
# siteconf = '/afs/crc.nd.edu/user/a/awoodard/coffeandbacon/analysis/siteconf'
wrapper = os.path.join(os.path.dirname(coffea.processor.funcx.__file__), 'data', 'siteconf')
wrapper = os.path.join(os.path.dirname(coffea.processor.funcx.__file__), 'data', 'wrapper.sh')

worker_init = '''
# source /cvmfs/sft.cern.ch/lcg/views/LCG_95apython3/x86_64-centos7-gcc7-opt/setup.sh

wrapper.sh bash

export PATH=~/.local/bin:$PATH
export PYTHONPATH=~/.local/lib/python3.6/site-packages:$PYTHONPATH
export FUNCX_TMPDIR=/tmp/{user}
export WORKER_TMPDIR=/tmp/{user}/workers

export X509_USER_PROXY=`pwd`/{proxy}

mkdir -p $FUNCX_TMPDIR

'''.format(user=os.environ['USER'], proxy=os.path.basename(proxy))

config = Config(
    scaling_enabled=False,
    worker_debug=True,
    cores_per_worker=1,
    strategy=SimpleStrategy(max_idletime=60000), # TOTAL HACK FIXME
    provider=CondorProvider(
        scheduler_options='stream_error=TRUE\nstream_output=TRUE\nTransferOut=TRUE\nTransferErr=TRUE',
        cores_per_slot=8,
        init_blocks=5,
        max_blocks=5,
        worker_init=worker_init,
        transfer_input_files=[proxy, siteconf, wrapper]
    ),
)
