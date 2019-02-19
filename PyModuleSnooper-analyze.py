from __future__ import print_function
from collections import Counter
import os
import sys

class PyModuleCounter:
    '''Counting imported module paths'''
    def __init__(self):
        self._counter = Counter()

    def parse_line(self, line):
        '''Extract list of imported module paths'''
        if not line.startswith('PyModuleSnooper'):
            return []
        timestamp, pathName, pythonInterpreter, modulePaths = line.split(';', 3)
        return modulePaths.split(';')
        
    def countline(self, line):
        modulePaths = self.parse_line(line)
        self._counter += Counter(modulePaths)

    def most_common(self, n):
        return self._counter.most_common(n)

    def items(self):
        return self._counter.items()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:", sys.argv[0], "<PyModuleSnooper Log File Path> [<LogFile2> <...> <LogFile-N>]")
        sys.exit(1)

    fnames = sys.argv[1:]
    for fname in fnames:
        if not os.path.isfile(fname):
            print(fname, "is not a file")
            sys.exit(1)
    
    counter = PyModuleCounter()
    for fname in set(fnames):
        with open(fname) as fp:
            for line in fp: counter.countline(line)

    print("Top 25 imports:")
    print(*counter.most_common(25), sep='\n')
