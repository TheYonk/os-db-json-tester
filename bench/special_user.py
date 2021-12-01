import json 

#import asyncio
import pg_yonk_library
import mysql_yonk_library
import argparse
#import multiprocessing
#from multiprocessing import Manager
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

 
activelist1 = []
activelist2 = []
activelist3 = []
activelist4 = []

activelist = [activelist1,activelist2,activelist3,activelist4]

chosen_lib =  mysql_yonk_library


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
    return 0

def event_spawn(eventid):
    return 0

def startup_workers():
    return 0
    
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
        except Exception as e:
            logging.error("error: %s", e)
            z = sys.exc_info()[0]
            logging.error("systems: %s",z )
               
if settings['type'] == 'postgresql' and settings['bench_active']==1:
        try:
            default_values = start_pg()
        except Exception as e:
            logging.error("error: %s", e)
            z = sys.exc_info()[0]
            logging.error("systems: %s",z )
            
count = 0
current_time = 0
start_time = 0    
start_time = time.perf_counter()
            
while (True):
   current_time = time.perf_counter() 
   logging.info('start time: %s',str(time.perf_counter()))
   logging.info('user special')    
   chosen_lib.single_user_actions_special(connect_string,1000,0.1,0,0,default_values['list_actors'],default_values['list_titles'],default_values['list_ids'], default_values['ai_myids'])
   count = count + 1;      
   logging.info('Finished time: %s , counter:  %s , time run: %s',str(time.perf_counter()), count, str(time.perf_counter()-current_time) )
   

current_time = time.perf_counter()
print("Started at..." + str( start_time))
print("Ended at..." + str( time.perf_counter()))