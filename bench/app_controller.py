import json 

import asyncio
import pg_yonk_library
import mysql_yonk_library
import argparse
import multiprocessing
from multiprocessing import Manager
import signal 
import logging
import errno
import os
import time
import sys

logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', dest='myfile', type=str, default="", help='the config file to use')

args = parser.parse_args()
print(args)

knobcounter = [0,0,0,0]
default_values = dict()
thread_list = [[],[],[],[]]

thread_list1 = []
thread_list2 = []
thread_list3 = []
thread_list4 = []

activelist = [Manager().dict(),Manager().dict(),Manager().dict(),Manager().dict()] 
activelist1 = Manager().dict()
activelist2 = Manager().dict()
activelist3 = Manager().dict()
activelist4 = Manager().dict()

chosen_lib =  pg_yonk_library


try:
   with open(args.myfile, "r") as read_file:
     settings = json.load(read_file)
   read_file.close();
except:
    logging.error('Problem reading file: %s ', args.myfile)

tag = settings['name']+str(settings['appnode'])+'-'+settings['host']

logging.info('read file %s, found the following settings: %s', args.myfile, settings)

MYDSN = "dbname=" + settings['database'] +" user="+ settings['username'] + " password=" + settings['password'] + " host=" + settings['host']

config = {
  'user': settings['username'],
  'password': settings['password'],
  'host': settings['host'],
  'database': settings['database'],
  'raise_on_warnings': False
}

def reload_config(myfile):
    #function to check and reload the config file
    try:
       with open(myfile, "r") as read_file:
         settings = json.load(read_file)
       read_file.close();
       return settings
    except:
        logging.error('Problem reading file: %s ', myfile)
        return -1
        
def start_mysql() : 
     print('Starting MySQL')
     global chosen_lib 
     chosen_lib =  mysql_yonk_library
     global connect_string
     connect_string = config
     default_values = chosen_lib.load_db(config)
     return default_values
     
def start_pg() : 
     print('Starting PG')    
     global chosen_lib 
     chosen_lib =  pg_yonk_library
     global connect_string
     connect_string = MYDSN
     default_values = chosen_lib.load_db(connect_string)
     return default_values
     
def stop_mysql() : 
     print('Stop MySQL')

def stop_pg() : 
     print('Stop PG')   
     
def active_threads_list():
     return 'G:(' + str(len(thread_list[0])) + ') R: ('+ str(len(thread_list[1])) + ') IU: ('+ str(len(thread_list[2])) + ') LT: ('+ str(len(thread_list[3])) +') '

def spawn_app_nodes(count,wid):
    
     worker_threads = ['chosen_lib.single_user_actions_v2','chosen_lib.report_user_actions','chosen_lib.insert_update_delete','chosen_lib.long_transactions']
     worker_desc = ['General Website','Reporting','Chat','Long Transactions']
    
     global activelist
     global thread_list
     if count > 0:
        for x in range(count):
            process = multiprocessing.Process(target=eval(worker_threads[wid]), args=(connect_string,1000,0.1,0,0,default_values['list_actors'],default_values['list_titles'],default_values['list_ids'], default_values['ai_myids'],activelist[wid]), daemon=True)
            process.start()
            activelist[wid][process.pid]=1
            logging.info('Started %s Workload as Pid %s', worker_desc[wid], process.pid)
            thread_list[wid].append(process)
            logging.info('%s Workload at count: %s', worker_desc[wid], str(len(thread_list[wid])) )                    
     if count < 0:
      for x in range(abs(count)):
          if (len(activelist[wid].keys()))> 0:
              process = thread_list[wid].pop()
              activelist[wid][process.pid]=0
              logging.info('%s Thread, Stopping Pid %s', worker_desc[wid], process.pid)
              logging.info('%s Workload at count: %s', worker_desc[wid], str(len(thread_list[wid])) )                    
     os.system('pmm-admin annotate "' + worker_desc[wid] + ' Changed: ' + active_threads_list() + '" --tags "Benchmark, Workload Change,'+ tag +'"')
     logging.debug('%s Workload at count: %s', worker_desc[wid], str(len(thread_list[wid])) )
    
def event_spawn(eventid):
    return 0

def startup_workers():
    spawn_app_nodes(settings['website_workload'],0)
    spawn_app_nodes(settings['reporting_workload'],1)
    spawn_app_nodes(settings['comments_workload'],2)
    spawn_app_nodes(settings['longtrans_workload'],3)
    
def full_stop_workload():
    global activelist
    global thread_list
    for key in activelist[0]:
        activelist[0][key]=0
    thread_list[0] = []
    
    for key in activelist[1]:
        activelist[1][key]=0
    thread_list[1] = []
    
    for key in activelist[2]:
        activelist[2][key]=0
    thread_list[2] = []
    
    for key in activelist[3]:
        activelist[3][key]=0
    thread_list[3] = []
    os.system('pmm-admin annotate "Full Stop Benchmark" --tags "Benchmark, Stop,'+ tag +'"')
    logging.info('Issued Deactivate Benchmark, Full Stop')
    
      
if settings['type'] == 'mysql' and settings['bench_active']==1:
        try:
            default_values = start_mysql()
            os.system('pmm-admin annotate "Full MySQL Start" --tags "MySQL, Benchmark, Start-Stop,'+ tag +'"')
        except Exception as e:
            logging.error("error: %s", e)
            z = sys.exc_info()[0]
            logging.error("systems: %s",z )
               
if settings['type'] == 'postgresql' and settings['bench_active']==1:
        try:
            default_values = start_pg()
            os.system('pmm-admin annotate "Full PG Start" --tags "PostgreSQL, Benchmark, Start-Stop,'+ tag +'"')
        except Exception as e:
            logging.error("error: %s", e)
            z = sys.exc_info()[0]
            logging.error("systems: %s",z )
            
if settings['bench_active']==1:            
   startup_workers()
               
last_settings = settings;

try: 
 while True:
    try:
        new_settings = reload_config(args.myfile)
    except: 
        logging.warn('problem with settings file')
        
    if (last_settings != new_settings):
        logging.info('Config Change Detected!')
        logging.info(active_threads_list())
        logging.info('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        logging.debug('last settings : %s', last_settings)
        logging.info('=======')
        logging.debug('new settings : %s', new_settings)
        logging.debug('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        try:
         if (new_settings['bench_active'] == 1 and last_settings['bench_active']==0):
            if settings['type'] == 'mysql':
                    try:
                        default_values = start_mysql()
                        os.system('pmm-admin annotate "Full MySQL Start" --tags "MySQL, Benchmark, Start-Stop,'+ tag +'"')
                    except:
                        logging.error('Some Error')
               
            if settings['type'] == 'postgresql':
                    try:
                        default_values = start_pg()
                        os.system('pmm-admin annotate "Full PG Start" --tags "PostgreSQL, Benchmark, Start-Stop,'+ tag +'"')
                    except:
                        logging.error('Some Error')
            startup_workers()
            logging.info(active_threads_list())
         else :
            if (new_settings['bench_active'] == 0 and last_settings['bench_active']==1):
                full_stop_workload();
            else :
                if (new_settings['website_workload'] - last_settings['website_workload'] != 0):
                    spawn_app_nodes(new_settings['website_workload'] - last_settings['website_workload'],0)
                if (new_settings['reporting_workload'] - last_settings['reporting_workload'] != 0):
                    spawn_app_nodes(new_settings['reporting_workload'] - last_settings['reporting_workload'],1) 
                if (new_settings['comments_workload'] - last_settings['comments_workload'] != 0):
                    spawn_app_nodes(new_settings['comments_workload'] - last_settings['comments_workload'],2)
                if (new_settings['longtrans_workload'] - last_settings['longtrans_workload'] != 0):
                    spawn_app_nodes(new_settings['longtrans_workload'] - last_settings['longtrans_workload'],3)   
         logging.info(active_threads_list())
        except Exception as e:
            logging.error('unknown issue')
            logging.error("error: %s", e)
            z = sys.exc_info()
            exc_type, exc_obj, exc_tb = sys.exc_info()
            logging.error("systems: %s",z )
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logging.error(exc_type, fname, exc_tb.tb_lineno)
    last_settings = new_settings         
    time.sleep(5)
except KeyboardInterrupt: 
    logging.info('Starting Shutdown!')
    full_stop_workload()
    time.sleep(5)
except Exception as e:
    logging.error('unknown shutdown issue')
    logging.error("error: %s", e)
    z = sys.exc_info()
    exc_type, exc_obj, exc_tb = sys.exc_info()
    logging.error("systems: %s",z )
    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    logging.error(exc_type, fname, exc_tb.tb_lineno)
    
        

  