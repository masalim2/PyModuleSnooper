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
import logging
import os
import socket
import sys

LOGFILE_ROOT = os.path.join('/projects', 'datascience', 'PyModuleSnooper', 'log')
LOGFILE_ROOT = os.path.expanduser('~/PyModuleSnooper/log')

def get_logger():
    '''Set up logger to emit message to system log facility'''
    extra_logging_fields = {'python':sys.executable,}
    logger = logging.getLogger("PyModuleSnooper")
    logger.propagate = False
    logger.setLevel(logging.INFO)

    todays_date = datetime.now().strftime('%m-%d-%Y')
    log_path = os.path.join(LOGFILE_ROOT, todays_date)
    handler_file = logging.FileHandler(log_path)

    formatter = logging.Formatter(
        'PyModuleSnooper: ts=%(asctime)s;pathName=%(pathname)s;python=%(python)s;modules=%(message)s'
    )
    handler_file.formatter = formatter
    logger.addHandler(handler_file)
    logger = logging.LoggerAdapter(logger, extra_logging_fields)
    return logger

def emit_log(modules_list):
    '''Log message containing loaded modules path list'''
    logger = get_logger()
    message = ';'.join(modules_list)
    logger.info(message)

def get_module_path(m):
    '''Resolve path from a module object'''
    if hasattr(m, '__path__') and len(m.__path__) > 0:
        return list(m.__path__)[0]
    elif hasattr(m, '__file__'):
        return m.__file__
    else:
        return None

def is_mpi_rank_nonzero():
    '''False if not using mpi4py, or MPI has been finalized, or MPI has
    not been initialized, or rank is 0. Otherwise, returns True if rank > 0.'''
    MPI = None
    if 'mpi4py' in sys.modules:
        if hasattr(sys.modules['mpi4py'], 'MPI'):
            MPI = sys.modules['mpi4py'].MPI
    if MPI is None:
        return False

    # If finalized or not initialized, we can't get rank, so return
    if MPI.Is_finalized():
        return False
    elif not MPI.Is_initialized():
        return False
    else:
        return MPI.COMM_WORLD.Get_rank() > 0

def inspect_and_log():
    '''Grab paths of all loaded modules and log them'''
    if is_mpi_rank_nonzero():
        return
    try:
        modules = [get_module_path(m) for m in sys.modules.values()]
    except:
        return
    modules = list(set([m for m in modules if m is not None]))
    emit_log(modules)

if not os.environ.get('DISABLE_PYMODULE_SNOOP', False):
    atexit.register(inspect_and_log)
