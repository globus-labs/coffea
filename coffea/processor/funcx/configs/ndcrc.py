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
siteconf = os.path.join(os.path.dirname(coffea.processor.funcx.__file__), 'data', 'siteconf')
wrapper = os.path.join(os.path.dirname(coffea.processor.funcx.__file__), 'data', 'wrapper.sh')

worker_init = '''

source ~/.bashrc
conda activate funcx

export X509_CERT_DIR=`pwd`/certificates
export PATH=~/.local/bin:$PATH
export PYTHONPATH=~/.local/lib/python3.6/site-packages:$PYTHONPATH
export FUNCX_TMPDIR=/tmp/{user}

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
        cores_per_slot=16,
        init_blocks=int(os.environ['blocks']),
        max_blocks=int(os.environ['blocks']),
        worker_init=worker_init,
        transfer_input_files=[proxy, siteconf, wrapper, os.environ['X509_CERT_DIR']]
    ),
)
