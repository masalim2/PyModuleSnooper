#!/usr/bin/env python
import os,sys,json,time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import concurrent.futures
import multiprocessing as mp
import argparse,logging

DEFAULT_NUM_PROCS = int(mp.cpu_count() * 0.9)
DEFAULT_YEARS = '2020'
DEFAULT_MONTHS = ''
DEFAULT_DAYS = ''
DEFAULT_OUTPUT = 'output.csv.gz'
DEFAULT_EXCLUDED_FILENAME = 'exclude_modules.json'
DEFAULT_SYSTEM_NODES_FILENAME = 'system_nodes.json'
DEFAULT_SOURCE_MAP_FILENAME = 'source_map.json'

logger = logging.getLogger(__name__)
exclude_modules = None
system_nodes = None
gconfig = {}

def main():
   global exclude_modules,system_nodes,gconfig
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging_format = '%(asctime)s %(levelname)s:%(name)s:%(message)s'
   logging_datefmt = '%Y-%m-%d %H:%M:%S'
   logging_level = logging.INFO
   
   parser = argparse.ArgumentParser(description='This script will walk the PyModuleSnooper log directory structure, parse the json files therein, and dump compressed format files.')
   parser.add_argument('-l','--logdir',help='Path to the PyModuleSnooper log files.',required=True)
   parser.add_argument('-n','--numprocs',help='Number of parallel processes to \
                       use to parse json files. [DEFAULT=%s]' % DEFAULT_NUM_PROCS,
                       type=int,default=DEFAULT_NUM_PROCS)
   parser.add_argument('-y','--years',help='Years to include, separated by comma. [DEFAULT=%s]' % DEFAULT_YEARS,
                       default=DEFAULT_YEARS)
   parser.add_argument('-m','--months',help='Months to include, separated by comma. [DEFAULT=%s]' % DEFAULT_MONTHS,
                       default=DEFAULT_MONTHS)
   parser.add_argument('-d','--days',help='Days of the Month to include, separated by comma. \
                       [DEFAULT=%s]' % DEFAULT_DAYS,default=DEFAULT_DAYS)
   parser.add_argument('-o','--output',help='Output data file name. Written as gzipped CSV. \
                       [DEFAULT=%s' % DEFAULT_OUTPUT,default=DEFAULT_OUTPUT)

   parser.add_argument('--excluded',help='Path to a json file containing a list of modules to exclude from \
                       the output. [DEFAULT=%s' % DEFAULT_EXCLUDED_FILENAME,default=DEFAULT_EXCLUDED_FILENAME)
   parser.add_argument('--sysnodes',help='Path to a json file containing a dictionary of lists that map node \
                       names to HPC names. [DEFAULT=%s' % DEFAULT_SYSTEM_NODES_FILENAME,default=DEFAULT_SYSTEM_NODES_FILENAME)
   parser.add_argument('--srcmap',help='Path to which to dump the source map which labels each python environment. [DEFAULT=%s' % DEFAULT_SOURCE_MAP_FILENAME,default=DEFAULT_SOURCE_MAP_FILENAME)

   parser.add_argument('--debug', dest='debug', default=False, action='store_true', help="Set Logger to DEBUG")
   parser.add_argument('--error', dest='error', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--warning', dest='warning', default=False, action='store_true', help="Set Logger to ERROR")
   parser.add_argument('--logfilename',dest='logfilename',default=None,
                       help='if set, logging information will go to file')
   args = parser.parse_args()

   if args.debug and not args.error and not args.warning:
      logging_level = logging.DEBUG
   elif not args.debug and args.error and not args.warning:
      logging_level = logging.ERROR
   elif not args.debug and not args.error and args.warning:
      logging_level = logging.WARNING

   logging.basicConfig(level=logging_level,
                       format=logging_format,
                       datefmt=logging_datefmt,
                       filename=args.logfilename)
   
   if len(args.years) > 0:
      years  = [int(x) for x in args.years.split(',')]
   else:
      years = []
   if len(args.months) > 0:
      months = [int(x) for x in args.months.split(',')]
   else:
      months = []
   if len(args.days) > 0:
      days   = [int(x) for x in args.days.split(',')]
   else:
      days = []

   logger.info('logdir     = %s',args.logdir)
   logger.info('years      = %s',args.years)
   logger.info('months     = %s',args.months)
   logger.info('days       = %s',args.days)
   logger.info('output     = %s',args.output)
   logger.info('numprocs   = %s',args.numprocs)
   logger.info('excluded   = %s',args.excluded)
   logger.info('sysnodes   = %s',args.sysnodes)
   start = time.time()
   exclude_modules = json.load(open(args.excluded))
   system_nodes = json.load(open(args.sysnodes))
   gconfig['years'] = years
   gconfig['months'] = months
   gconfig['days'] = days
   gconfig['path'] = args.logdir

   ds = build_dataset(args.logdir,args.numprocs,years,months,days)

   ds.to_csv(args.output,index=False,compression='gzip')
   
   json.dump(gsource_map,open(args.srcmap,'w'),sort_keys=True, indent=3)

   logger.info('total run time: %10.2f',time.time() - start)


def commonize_source(source):
   try:
      if source.startswith('/soft/'):
         source = source.replace('/soft/','/lus/theta-fs0/software/')
      elif source.startswith('/projects/'):
         source = source.replace('/projects/','/lus/theta-fs0/projects/')
      elif source.startswith('/home/'):
         source = source.replace('/home/','/gpfs/mira-home/')

      if source.endswith('python3.8'):
         source = source.replace('python3.8','python')
      elif source.endswith('python3.7'):
         source = source.replace('python3.7','python')
      elif source.endswith('python3'):
         source = source.replace('python3','python')
      return source
   except:
      return ''


def parse_datafile(filename):
   try:
      data = json.load(open(filename))
   except:
      print(f'failed to parse filename: {filename}')
      return {}
   output_data = {}
   output_data['hostname'] = data['hostname']
   output_data['hpcname'] = 'NA'

   for system,nodes in system_nodes.items():
      output_data[system] = 0
      for node in nodes:
         if node in data['hostname']:
            output_data[system] = 1
            output_data['hpcname'] = system
            break
      if output_data[system]:
         break
   output_data['filename'] = filename
   output_data['source'] = commonize_source(data['sys.executable'])
   output_data['timestamp'] = pd.Timestamp(data['timestamp'])
   modules = []
   module_keys = data['modules'].keys()
   # remove submodules
   module_keys = [x.split('.')[0] for x in module_keys]
   # keep unique keys only
   module_keys = list(set(module_keys))
   
   # loop over modules, remove any from `/tmp`
   for module_name,module_filename in data['modules'].items():
      if module_filename is None:
         continue
      if module_filename.startswith('/tmp/'):
         try:
            module_keys.remove(module_name.split('.')[0])
         except ValueError:
            #print(module_name,module_filename)
            continue
         except KeyError:
            #print(module_name,module_filename)
            continue
   sys.stdout.flush()
   sys.stderr.flush()

   # remove excluded modules
   for module_name in exclude_modules:
      try:
         module_keys.remove(module_name)
      except ValueError:
         continue
      except KeyError:
         continue
   output_data['modules'] = list(module_keys)
   return output_data


def make_each_file_list(walk_inputs):
   root,dirs,files = walk_inputs
   years = gconfig['years']
   months = gconfig['months']
   days = gconfig['days']
   path = gconfig['path']

   filelist = []
   logger.debug('root: %s',root)
   rr = root.replace(path,'')
   logger.debug('rr: %s',rr)
   rr = rr.split('/')
   logger.debug('rr: %s',rr)
   if rr and len(rr) == 3:
      year = int(rr[0])
      month = int(rr[1])
      day = int(rr[2])
      if(((len(years) > 0 and year in years) or len(years) == 0) and
         ((len(months) > 0 and month in months) or len(months) == 0) and
         ((len(days) > 0 and day in days) or len(days) == 0)):
         logger.info('%04d-%02d-%02d',year,month,day)
         logger.info('%s #dirs: %s #files: %s',root,len(dirs),len(files))
         for file in files:
               filename = os.path.join(root,file)
               if os.stat(filename).st_size == 0:
                  continue
               
               filelist.append(filename)
   return filelist

def get_file_list(path,nprocs,years=[],months=[],days=[]):
   filelist = []
   logger.debug('get_file_list: path=%s years=%s months=%s days=%s',path,years,months,days)
   if not path.endswith('/'):
      path = path + '/'

   with concurrent.futures.ThreadPoolExecutor(max_workers=nprocs) as pool:
      for each_filelist in pool.map(make_each_file_list,os.walk(path),chunksize=100):
         filelist += each_filelist

   return filelist


def get_source_id(dataset):
   global gsource_map
   unique_source = list(set(dataset['source'].to_list()))
   gsource_map = {unique_source[i]:i for i in range(len(unique_source))}
   return dataset['source'].replace(gsource_map)


def build_dataset(path,nprocs,years=[],months=[],days=[]):
   #dataset = pd.DataFrame()
   filelist = get_file_list(path,nprocs,years,months,days)
   logger.info(f'{len(filelist)} files')
   with concurrent.futures.ThreadPoolExecutor(max_workers=nprocs) as pool:
      total_files = len(filelist)
      one_percent = int(total_files * 0.01)
      file_counter = 0
      start = time.time()
      outputs = []
      for data in pool.map(parse_datafile,filelist,chunksize=100):
         if len(data) > 0:
            outputs.append(data)
            file_counter += 1
            if file_counter % one_percent == 0:
               files_per_sec = one_percent / (time.time() - start)
               percent_done = int(file_counter / total_files * 100)
               logger.info('percent done: %3d%%   files/second: %10.2f',percent_done,files_per_sec)
               sys.stderr.flush()
               start = time.time()
   start = time.time()
   dataset = pd.DataFrame(outputs)
   logger.info('dataset created: %10.2f',time.time() - start)
   dataset['source_id'] = get_source_id(dataset)
   return dataset


if __name__ == "__main__":
   main()
