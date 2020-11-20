Installation
---------------

1. Modify `LOGFILE_ROOT` in sitecustomize.py to point at a writeable directory
2. Place sitecustomize.py in the lib/pythonX.Y/site-packages/ directory of your Python installation

How it works
-------------
*One* JSON-formatted log line with all loaded modules will be appended to a file in `LOGFILE_ROOT` named after today's date.
This is emitted on interpreter shutdown under most normal termination circumstances.  If `mpi4py` is 
loaded, only rank 0 will log.

Currently, a timestamp, the Python executable, sys.path, Cobalt environment variables, and dictionary containing all
loaded module paths is logged. This data can reach about 220K bytes per-log line when Tensorflow is imported, for instance.

Disable Snooping
----------------
Pass `-S` flag to Python interpreter to disable the `site` module and
`sitecustomize` hook entirely.  Alternatively, set the environment flag
`DISABLE_PYMODULE_LOG` (any non-empty string will disable it).

Analysis
---------
Run `./analyze.py <logfile-1> <...> <logfile-N>` to see a count of the most commonly imported modules on any given day(s).
Change `REPORT_DEPTH` to control how far the report will traverse subpackages.
