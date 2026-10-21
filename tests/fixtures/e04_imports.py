import importlib
import json
import logging
import logging.config
import multiprocessing as mp
from multiprocessing.process import BaseProcess
import os
import signal
import socket
import subprocess
import threading

# Existing caller configuration must survive both public and CLI imports.
logger=logging.getLogger('caller.owned');logger.setLevel(logging.ERROR);logger.propagate=False
handler=logging.StreamHandler();logger.addHandler(handler)
root=logging.getLogger();root.setLevel(logging.WARNING)
before=(dict(os.environ),list(threading.enumerate()),mp.get_start_method(allow_none=True),
        root.level,list(root.handlers),logger.level,list(logger.handlers),logger.propagate,
        {s:signal.getsignal(s) for s in signal.valid_signals() if s not in {signal.SIGKILL,signal.SIGSTOP}})
def forbidden(*args,**kwargs):raise AssertionError('import attempted process-wide setup or I/O')
socket.socket.__init__=forbidden
socket.socket.connect=socket.socket.connect_ex=socket.socket.bind=socket.socket.listen=forbidden
subprocess.Popen=BaseProcess.start=threading.Thread.start=forbidden
signal.signal=mp.set_start_method=logging.basicConfig=logging.config.dictConfig=logging.config.fileConfig=forbidden
for module in ('neo4j_graph_loader.schema','neo4j_graph_loader.planning','neo4j_graph_loader.cli'):
    importlib.import_module(module)
from neo4j_graph_loader.utils import metrics
assert metrics._process_metrics is None
assert before==(dict(os.environ),list(threading.enumerate()),mp.get_start_method(allow_none=True),
        root.level,list(root.handlers),logger.level,list(logger.handlers),logger.propagate,
        {s:signal.getsignal(s) for s in signal.valid_signals() if s not in {signal.SIGKILL,signal.SIGSTOP}})
print(json.dumps({'imports':'inert','signals':len(before[-1]),'threads':len(before[1]),'metrics_initialized':False}))
