import json

from funcx.sdk.client import FuncXClient

from coffea.processor.parsl.timeout import timeout

# @timeout
def derive_chunks(filename, treename, chunksize, ds, timeout=10):
# def derive_chunks(filename, treename, chunksize, ds):
    import uproot
    from collections.abc import Sequence

    uproot.XRootDSource.defaults["parallel"] = False

    afile = uproot.open(filename)

    tree = None
    if isinstance(treename, str):
        tree = afile[treename]
    elif isinstance(treename, Sequence):
        for name in reversed(treename):
            if name in afile:
                tree = afile[name]
    else:
        raise Exception('treename must be a str or Sequence but is a %s!' % repr(type(treename)))

    if tree is None:
        raise Exception('No tree found, out of possible tree names: %s' % repr(treename))

    nentries = tree.numentries
    return ds, treename, [(filename, chunksize, index) for index in range(nentries // chunksize + 1)]

# TODO have parsl executor import from here so it is not defined twice
# @timeout
def process(dataset, fn, treename, chunksize, index, stageout_url, timeout=None, flatten=True):
    import os
    import random
    import string
    import subprocess
    from collections.abc import Sequence

    import uproot
    import cloudpickle as cpkl
    import pickle as pkl
    import lz4.frame as lz4f

    from coffea import hist, processor
    from coffea.util import load, save
    from coffea.processor.accumulator import value_accumulator

    uproot.XRootDSource.defaults["parallel"] = False

    lz4_clevel = 16

    # instrument xrootd source
    if not hasattr(uproot.source.xrootd.XRootDSource, '_read_real'):

        def _read(self, chunkindex):
            self.bytesread = getattr(self, 'bytesread', 0) + self._chunkbytes
            return self._read_real(chunkindex)

        uproot.source.xrootd.XRootDSource._read_real = uproot.source.xrootd.XRootDSource._read
        uproot.source.xrootd.XRootDSource._read = _read

    processor_path = os.path.join(os.environ['FUNCX_TMPDIR'], 'processor')
    if not os.path.isfile(processor_path):
        command = 'xrdcp {}/processor {}'.format(stageout_url, processor_path)
        subprocess.call(command, shell=True)
    processor_instance = load(processor_path)

    afile = uproot.open(fn)

    tree = None
    if isinstance(treename, str):
        tree = afile[treename]
    elif isinstance(treename, Sequence):
        for name in reversed(treename):
            if name in afile:
                tree = afile[name]
    else:
        raise Exception('treename must be a str or Sequence but is a %s!' % repr(type(treename)))

    if tree is None:
        raise Exception('No tree found, out of possible tree names: %s' % repr(treename))

    df = processor.LazyDataFrame(tree, chunksize, index, flatten=flatten)
    df['dataset'] = dataset

    vals = processor_instance.process(df)
    if isinstance(afile.source, uproot.source.xrootd.XRootDSource):
        vals['_bytesread'] = value_accumulator(int) + afile.source.bytesread
    valsblob = lz4f.compress(pkl.dumps(vals), compression_level=lz4_clevel)


    output = os.path.join(
        os.environ['FUNCX_TMPDIR'],
        ''.join(random.choice(string.ascii_lowercase) for i in range(10))
    )
    save((valsblob, df.size, dataset), output)

    command = 'xrdcp {} {}'.format(output, stageout_url)
    subprocess.call(command, shell=True)
    os.unlink(output)

    return os.path.join(stageout_url, os.path.basename(output))

client = FuncXClient()
uuids = {}
for func in [derive_chunks, process]:
    f = timeout(func)
    uuids[func.__name__] = client.register_function(f)

with open('function_uuids.json', 'w') as f:
    f.write(json.dumps(uuids, indent=4, sort_keys=True))
