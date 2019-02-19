Installation
---------------

1. Modify `LOGFILE_ROOT` in sitecustomize.py to point at a writeable directory
2. Copy sitecustomize.py into the lib/pythonX.Y/site-packages/ directory of your Python installation

Usage
--------
A log line with all loaded modules will be emitted to a file in LOGFILE_ROOT named after today's date.
This is emitted on interpreter shutdown under most normal termination circumstances.  If `mpi4py` is 
loaded, only rank 0 will log.

Disable Snooping
----------------
Pass `-S` flag to Python interpreter to disable the `site` module and
`sitecustomize` hook entirely.  Alternatively, set the environment flag
`DISABLE_PYMODULE_SNOOP` (any non-empty string will disable it).

Analysis
---------
Run `./analyze.py <logfile-1> <...> <logfile-N>` to see a count of the most commonly imported modules on any given day(s).
