#!/usr/bin/env python
from __future__ import print_function
from collections import Counter
from datetime import datetime
import json
import os
import sys

TIME_FMT = '%m-%d-%Y %H:%M:%S.%f'
REPORT_DEPTH = 4

def load_ignore():
    here = os.path.dirname(__file__)
    ignore_fname = os.path.join(here, 'IGNORE_MODULES')
    if not os.path.isfile(ignore_fname):
        return []
    with open(ignore_fname) as f:
        modules = f.readlines()
    return [m.strip() for m in modules if len(m)>1]

class PyModuleCounter(Counter):
    def __init__(self):
        super().__init__()
        self.ignore_modules = load_ignore()
        self._prefixes = {}
        self._not_prefixes = []
        self._used_prefixes = []
    
    def used_prefixes(self):
        return [(k,self._prefixes[k]) for k in self._used_prefixes]

    def trim_prefix(self, modpath):
        for prefix, abbrev in self._prefixes.items():
            if modpath.startswith(prefix):
                modpath = modpath.replace(prefix, abbrev)
                if prefix not in self._used_prefixes:
                    self._used_prefixes.append(prefix)
                break
        return '/'.join(modpath.split('/')[:REPORT_DEPTH])

    def load_prefixes(self, prefix_list):
        for path in prefix_list:
            if path in self._prefixes or path in self._not_prefixes: 
                continue
            elif not os.path.isdir(path):
                self._not_prefixes.append(path)
            else:
                self._prefixes[path] = 'P{}'.format(len(self._prefixes))

    def _parse_line(self, line):
        d = json.loads(line)
        timestamp = datetime.strptime(d['timestamp'], TIME_FMT)
        python_exe = d['sys.executable']
        sys_paths = d['sys.path']
        cobalt_envs = d['cobalt_envs']
        modules_dict = d['modules']

        self.load_prefixes(sys_paths)
        counted_path_set = {
            self.trim_prefix(module_path) 
            for module_name,module_path in modules_dict.items()
            if module_name not in self.ignore_modules
        }
        return counted_path_set
        
    def countline(self, line):
        '''Increment internal count from modules in line'''
        modulePaths = self._parse_line(line)
        self += Counter(modulePaths)


def main(*log_files):
    counter = PyModuleCounter()
    for fname in set(log_files):
        with open(fname) as fp:
            for line in fp: counter.countline(line)

    print("Prefixes:")
    print(*counter.used_prefixes(), sep='\n')
    print("Imports:")
    print(*counter.items(), sep='\n')

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:", sys.argv[0], "<PyModuleSnooper Log File Path> [<LogFile2> <...> <LogFile-N>]")
        sys.exit(1)

    fnames = sys.argv[1:]
    for fname in fnames:
        if not os.path.isfile(fname):
            print(fname, "is not a file")
            sys.exit(1)

    main(*fnames)
