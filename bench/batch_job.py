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
parser.add_argument('-t', '--time', dest='sleeptime', type=int, default="600", help='time to wait between runs')

args = parser.parse_args()
print(args)
global connect_string


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


#def start_mysql() : 
#     print('Starting MySQL')
#     global chosen_lib 
#     chosen_lib =  mysql_yonk_library
#     global connect_string
#     connect_string = config
#     default_values = chosen_lib.load_db(config)
#     return default_values
     
#def start_pg() : 
#     print('Starting PG')    
#     global chosen_lib 
#     chosen_lib =  pg_yonk_library
#     global connect_string
#     connect_string = MYDSN
#     default_values = chosen_lib.load_db(connect_string)
#     return default_values
     
def stop_mysql() : 
     print('Stop MySQL')

def stop_pg() : 
     print('Stop PG')   
     
      
if settings['type'] == 'mysql' and settings['bench_active']==1:
        try:
            #default_values = start_mysql()
            default_values = 0
            connect_string = config
        except Exception as e:
            logging.error("error: %s", e)
            z = sys.exc_info()[0]
            logging.error("systems: %s",z )
               
if settings['type'] == 'postgresql' and settings['bench_active']==1:
        try:
            #default_values = start_pg()
            default_values = 0
            connect_string = MYDSN
        except Exception as e:
            logging.error("error: %s", e)
            z = sys.exc_info()[0]
            logging.error("systems: %s",z )
            
count = 0
current_time = 0
start_time = 0    
start_time = time.perf_counter()
            
while True:
   logging.info('start time: %s',str(time.perf_counter()))
   logging.info('Running Load Cout History')
   chosen_lib.func_load_voting_count_hisory(connect_string)
   logging.info('End time: %s',str(time.perf_counter()))
   logging.info('sleeping ' + str(args.sleeptime))
   time.sleep(args.sleeptime) 
        


current_time = time.perf_counter()
print("Started at..." + str( start_time))
print("Ended at..." + str( time.perf_counter()))