'''PyModuleSnooper: log loaded module paths on CPython shutdown

PyRats relies on AST and parsing the .py file *before* interpreter begins
execution.  This precludes catching dynamic imports and does not *follow*
dependencies.

This approach uses atexit() to inspect sys.modules and log all loaded modules
upon interpreter shutdown.  It should log under most normal termination
circumstances. Atexit() should be preferred over registering signal handlers,
because users may register their own handlers.

* This will run on SIGINT (ctrl+c)
  or on any non-fatal Python exceptions (SyntaxError, ValueError, etc...)
* It will NOT run for unhandled signals (SIGTERM, SIGKILL)
* It will NOT run if os._exit() is invoked directly
* It will NOT run if CPython interpreter itself crashes
* Refer to https://docs.python.org/3.6/library/atexit.html
'''

import atexit
from datetime import datetime
import json
import logging
import os
import socket
import sys

DATETIME_FMT = '%m-%d-%Y %H:%M:%S.%f'
LOGFILE_ROOT = os.path.join('/lus', 'theta-fs0', 'logs', 'pythonlogging', 'module_usage')

def date_fmt(n):
    return "%02d" % n

class DictLogger:
    '''Set up logger to emit message to system log facility'''
    def __init__(self):
        now = datetime.now()
        self._info = {
            'timestamp' : now.strftime(DATETIME_FMT),
            'sys.executable': sys.executable,
            'sys.argv': sys.argv,
            'sys.path': sys.path,
            'env': os.environ.copy(),
            'hostname': socket.gethostname(),
            'pid': os.getpid(),
        }

        logger = logging.getLogger("PyModuleSnooper")
        logger.propagate = False
        logger.setLevel(logging.INFO)

        # LOGROOT/year/month/day/hostname.PID.hour.minute.second.m
        year,month,day = map(date_fmt, (now.year,now.month,now.day))
        job_id = os.environ.get('COBALT_JOBID', 'no-ID')
        log_dir = os.path.join(LOGFILE_ROOT, year, month, day)

        fname = '{}.{}.{}'.format(
            socket.gethostname(), os.getpid(), now.strftime('%H.%M.%S.%f')
        )
        log_path = os.path.join(log_dir, fname)
        handler_file = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(message)s')
        handler_file.formatter = formatter
        logger.addHandler(handler_file)
        self._logger = logger

    def log_modules(self, module_paths, module_versions):
        self._info['modules'] = module_paths
        self._info['versions'] = module_versions
        self._logger.info(json.dumps(self._info))

def is_mpi_rank_nonzero():
    '''False if not using mpi4py, or MPI has been finalized, or MPI has
    not been initialized, or rank is 0. Otherwise, returns True if rank > 0.'''
    MPI = None
    if 'mpi4py' in sys.modules:
        if hasattr(sys.modules['mpi4py'], 'MPI'):
            MPI = sys.modules['mpi4py'].MPI

    if MPI is None: 
        return False
    elif hasattr(MPI, "Is_finalized") and MPI.Is_finalized():
        return False
    elif hasattr(MPI, "Is_initialized") and not MPI.Is_initialized():
        return False
    elif hasattr(MPI, "COMM_WORLD"):
        return MPI.COMM_WORLD.Get_rank() > 0
    else:
        return False

def inspect_and_log():
    '''Grab paths of all loaded modules and log them'''
    if is_mpi_rank_nonzero(): return
    if os.environ.get('DISABLE_PYMODULE_LOG', False): return

    logger = DictLogger()
    module_paths = {
        module_name : module.__file__
        for module_name, module in sys.modules.copy().items()
        if hasattr(module, '__file__')
    }
    module_versions = {
        module_name: str(getattr(module, "__version__", None))
        for module_name, module in sys.modules.copy().items()
        if module_name in module_paths
    }
    logger.log_modules(module_paths, module_versions)

if not os.environ.get('DISABLE_PYMODULE_LOG', False):
    atexit.register(inspect_and_log)
