# this script has a very simple purpose.  Loop through all Json in the config folder.  Spawn 1 worker/controller per json.  Exit.  Controller takes over the heavy lifting
# this is purposfully single threaded, to avoid too many changes going in at once 


import json 
import logging
import os
import glob
import lib.pg_workload_lib
import lib.mysql_workload_lib
import sys
import time

logging.basicConfig(level=logging.DEBUG)

valid = 0

directory = './events/'
event_count = {}


with open('bench_setup.json', "r") as setup_file:
    try:
       setup = json.load(setup_file)
       valid = 1
    except: 
        logging.error('found %s does not contain a valid Json schema', setup_file )            
        valid = 0


def run_event(settings):
   MYDSN = "dbname=" + settings['database'] +" user="+ settings['username'] + " password=" + settings['password'] + " host=" + settings['host']

   config = {
   'user': settings['username'],
   'password': settings['password'],
   'host': settings['host'],
   'database': settings['database'],
   'raise_on_warnings': False
   }
   logging.info('Starting the processing of %s ', settings['event'] )
   
   if (settings['type'] == 'mysql'):
            chosen_lib =  lib.mysql_workload_lib
            connect_string = config
            myfunc = (setup['mysql']['events'][settings['event']])
            logging.debug('running function:  %s ', myfunc )
            x = eval('chosen_lib.'+myfunc)(connect_string)
               
   if (settings['type'] == 'postgresql'):
            chosen_lib =  lib.pg_workload_lib
            connect_string = MYDSN
            myfunc = (setup['postgresql']['events'][settings['event']])
            logging.debug('running function:  %s ', myfunc )
            x = eval('chosen_lib.'+myfunc)(connect_string)
            #chosen_lib.myfunc(connect_string)
            
            

while True:
  files = glob.glob("./events/*.json")
  files.sort(key=os.path.getmtime)

  for myfile in files:
    logging.debug('Found the following files:  %s', files)        
    #if (myfile.path.endswith(".json")):
    #    logging.info('found %s : starting validation', myfile.path )
    try :
       with open(myfile, "r") as read_file:
          data = json.load(read_file)
    except Exception as e:
       logging.error("error: %s", e)
       z = sys.exc_info()[0]
       logging.error("systems: %s",z )
       os.rename(myfile,myfile+'.failed')
           
    try:    
       tag = data['name']+'-'+data['host']
       logging.debug('checking %s', myfile ) 
       logging.debug('found event %s', data['event'] ) 
       os.system('pmm-admin annotate " starting '+  data['event']  +'" --tags "Benchmark, Schema Change,'+ tag +'"')
       run_event(data)
       os.system('pmm-admin annotate " finishing '+  data['event']  +'" --tags "Benchmark, Schema Change,'+ tag +'"')
       logging.debug('Finished processing event %s', data['event'] )
       os.rename(myfile,myfile+'.finished')        
    except Exception as e:
       logging.error("error: %s", e)
       z = sys.exc_info()[0]
       logging.error("systems: %s",z )
       os.rename(myfile,myfile+'.failed')        
       
    
    #read_file.close()    
  time.sleep(5)
               
        
# need to validate the Json here, simple thing but skipping for now.  

        
             