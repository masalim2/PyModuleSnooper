#!/usr/bin/env python
import os,sys,json,time,glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import concurrent.futures
import multiprocessing as mp
import argparse,logging

DEFAULT_NUM_PROCS = int(mp.cpu_count() * 0.9)
DEFAULT_YEARS = '2021'
DEFAULT_MONTHS = ''
DEFAULT_DAYS = ''
DEFAULT_OUTPUT = 'output.png'

logger = logging.getLogger(__name__)


def main():
   global exclude_modules,system_nodes,gconfig
   ''' simple starter program that can be copied for use when starting a new script. '''
   logging_format = '%(asctime)s %(levelname)s:%(name)s:%(message)s'
   logging_datefmt = '%Y-%m-%d %H:%M:%S'
   logging_level = logging.INFO
   
   parser = argparse.ArgumentParser(description='This script will walk the PyModuleSnooper log directory structure, parse the json files therein, and dump compressed format files.')
   parser.add_argument('-l','--logdir',help='Path to the csv.gz files.',required=True)
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

   start = time.time()

   filelist = get_file_list(args.logdir,years,months,days)

   logger.info('number of files: %s',len(filelist))

   dataset = build_dataset(filelist,args.numprocs)

   source_map = get_source_id(dataset)
   logger.info('source_map: \n %s\n',source_map)

   plot_dataset(dataset,args.output)

   logger.info('total run time: %10.2f',time.time() - start)


def plot_dataset(dataset,output_filename=None):
   fig,ax = plt.subplots(2,2,figsize=(12,12),dpi=80)

   modules = ['balsam','tensorflow','torch','horovod','scipy','numpy','h5py','sklearn','keras']
   plot_module_usage_by_day(dataset,ax[0,0],modules)

   plot_source(dataset,ax[0,1])

   plot_machine_by_day(dataset,ax[1,0],['thetaknl','thetagpu'],{'thetaknl':'blue','thetagpu':'green'})

   plot_most_used_modules(dataset,ax[1,1])

   fig.tight_layout()

   if output_filename:
      logger.info('ouput plot to %s',output_filename)
      fig.savefig(output_filename)


def make_each_file_list(args):
   walk_inputs,years,months,days,path = args
   root,dirs,files = walk_inputs

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


def get_file_list(path,years=[],months=[],days=[]):
   
   filelist = glob.glob(path + '/*.csv.gz')

   output_filelist = []
   for file in filelist:
      filename = os.path.basename(file).replace('.csv.gz','')
      parts = filename.split('-')
      year = int(parts[0])
      month = int(parts[1])
      day = int(parts[2])
      logger.debug('filename: %s  parts = %s',file,parts)
      if((year in years or len(years) == 0) and
         (month in months or len(months) == 0) and
         (day in days or len(days) == 0)):
         output_filelist.append(file)

   return output_filelist


def parse_datafile(filename):
   dataset = pd.read_csv(filename,compression='gzip')
   try:
      dataset['modules'] = dataset['modules'].str.replace("'",'"').apply(json.loads)
   except:
      logger.info('did not replace quotes in modules: %s',dataset['modules'])
   dataset['timestamp'] = pd.to_datetime(dataset['timestamp'])
   return dataset


def build_dataset(filelist,nprocs):
   output_dataset = pd.DataFrame()
   with concurrent.futures.ThreadPoolExecutor(max_workers=nprocs) as pool:
      for data in pool.map(parse_datafile,filelist,chunksize=100):
         output_dataset = output_dataset.append(data,ignore_index=True)

   return output_dataset


def get_source_id(dataset):
   
   unique_source = list(set(dataset['source'].to_list()))
   source_map = {unique_source[i]:i for i in range(len(unique_source))}
   dataset['source_id'] = dataset['source'].replace(source_map)

   return source_map


def plot_module_usage_by_day(dataset,ax,module_list,colors=None):
   data = []
   dataset['modules'].fillna('',inplace=True)
   for module in module_list:
      module_entries = dataset[dataset['modules'].apply(lambda x: module in x)]
      use_per_day = dataset['timestamp'].groupby(module_entries['timestamp'].dt.day).count()
      use_per_day = use_per_day.rename(module)
      data.append(use_per_day)
   data = pd.concat(data,axis=1)
   data.plot(kind='line',ax=ax,color=colors)
   ax.set_xlabel('day of the month')
   ax.set_yscale('log')
   ax.legend(ncol=int(len(module_list)/3) + 1)
#     ax.set_xlim(1,31)
#     days = [ x for x in range(1,32) ]
#     ax.set_xticks(days)
#     ax.set_xticklabels(days)
   min_date = dataset['timestamp'].min()
   max_date = dataset['timestamp'].max()
   ax.set_title('Covers ' + str(min_date.date()) + ' to ' + str(max_date.date()))


def plot_source(dataset,ax):
   newdata = dataset.groupby(['source_id','source']).size().reset_index().rename(columns={0:'count'})
   newdata[['source_id','count']].plot(x='source_id',y='count',kind='bar',ax=ax,logy=True,legend=False)
   #dataset.groupby(dataset['source_id'])['timestamp'].count().plot(kind='bar',ax=ax,logy=True)
   ax.set_xlabel('python source module ID')
   min_date = dataset['timestamp'].min()
   max_date = dataset['timestamp'].max()
   ax.set_title('Covers ' + str(min_date.date()) + ' to ' + str(max_date.date()))
   pd.options.display.max_colwidth = 500
   print(newdata[['source_id','source']])


def plot_machine_by_day(dataset,ax,machine_list,colors=None):
   data = []
   for machine in machine_list:
      machine_entries = dataset[dataset[machine] == 1]
      use_per_day = dataset['timestamp'].groupby(machine_entries['timestamp'].dt.day).count()
      use_per_day = use_per_day.rename(machine)
      data.append(use_per_day)
   data = pd.concat(data,axis=1)
   data.plot(kind='line',ax=ax,color=colors)
   ax.set_xlabel('day of the month')
   ax.set_yscale('log')
   ax.legend()
   min_date = dataset['timestamp'].min()
   max_date = dataset['timestamp'].max()
   ax.set_title('Covers ' + str(min_date.date()) + ' to ' + str(max_date.date()))


def plot_most_used_modules(dataset,ax,top_n=20):
    
   mods = dataset['modules']
   mod_dict = {}
   for modlist in mods:
      for entry in modlist:
         if entry in mod_dict.keys():
            mod_dict[entry] += 1
         else:
            mod_dict[entry] = 1

   ds = pd.DataFrame({'module':mod_dict.keys(),'occurance':mod_dict.values()})

   ds = ds.sort_values('occurance',ascending=False)

   ds[0:top_n].plot(kind='bar',ax=ax,legend=False)
   ax.set_xlabel(f'top {top_n} modules')
   ax.set_yscale('log')
   ax.set_xticklabels(ds['module'][0:top_n])

   min_date = dataset['timestamp'].min()
   max_date = dataset['timestamp'].max()
   ax.set_title('Covers ' + str(min_date.date()) + ' to ' + str(max_date.date()))

   return ds


if __name__ == "__main__":
   main()
