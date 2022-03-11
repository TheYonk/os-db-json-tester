import json 

import asyncio
import lib.pg_workload_lib
import lib.mysql_workload_lib
import argparse
import multiprocessing
from multiprocessing import Manager
import signal 
import logging
import errno
import os
import time
import sys
import signal


parser = argparse.ArgumentParser()
parser.add_argument('-f', '--file', dest='myfile', type=str, default="", help='the config file to use')
parser.add_argument('-np', '--no-pmm', dest='nopmm',action="store_true", default=0, help='skip-pmm annotations')
parser.add_argument('-v', '--verbose', dest='verbose',action="store_true", default=0, help='verbose output, debug mode')
parser.add_argument('-s', '--special', dest='special',action="store_true", default=0, help='using the special flag you can use the u and n flags to exectute the user function (u) via (n) threads')
parser.add_argument('-u', '--user-function', dest='function', type=str, default="", help='the function you want to call')
parser.add_argument('-n', '--number-threads', dest='threads', type=int, default=1, help='the number of desired threads')
parser.add_argument('-t', '--time', dest='time', type=int, default=-1, help='the legnth of time to run the app controller, -1 is forever the default')
parser.add_argument('-r', '--restart', dest='verbose',action="store_true", default=0, help='do you want to restart all threads every 60 minutes, sometimes long overnight runs run into unforseen issues')
parser.add_argument('-mc', '--mysql-client', dest='myclient',type=str, default="connector", help='the mysql client driver to use, [connector,mysqlclient]')
user_function_list = {'website_workload':'single_user_actions_v2', 'reporting_workload':'report_user_actions', 'comments_workload':'insert_update_delete','longtrans_workload':'long_transactions', 'special_workload':'single_user_actions_special_v2', 'read_only_workload':'read_only_user_actions','json_ro_workload':'read_only_user_json_actions','list_workload':'multi_row_returns', 'logging_workload': 'insert_update_logs' }
                       

args = parser.parse_args()
print(args)

if args.verbose:
   logging.basicConfig(level=logging.DEBUG)
else: 
   logging.basicConfig(level=logging.INFO)

if args.special:
   if args.function in user_function_list.keys():
      logging.info("Setup for user function: %s", args.function)
      logging.info("Time to run: %s", str(args.time))
      logging.info("# of threads: %s", str(args.threads))
   else:
      keys = user_function_list.keys()
      logging.error("User Function (-u) not found.  Valid functions are:  %s",keys) 
      sys.exit()      

if (args.myfile ==""):
    logging.error("no -f specified. You need a condig file under app_config/") 
    sys.exit()
 
knobcounter = [0,0,0,0]
default_values = dict()
thread_list = []
mysql_driver = 'connector'

#thread_list1 = []
#thread_list2 = []
#thread_list3 = []
#thread_list4 = []

activelist = []
worker_threads = []
wid_lookup = dict()

for key in user_function_list:
    activelist.append(Manager().dict())
    worker_threads.append('chosen_lib.'+user_function_list[key])
    wid_lookup[key] = len(thread_list)
    thread_list.append([])
activelist = [Manager().dict(),Manager().dict(),Manager().dict(),Manager().dict(),Manager().dict(),Manager().dict(),Manager().dict(),Manager().dict(),Manager().dict(),Manager().dict()] 
timinglist = Manager().list([0,0,0,0,0,0,0,0,0,0,0])
countlist = Manager().list([0,0,0,0,0,0,0,0,0,0,0])
logging.debug(wid_lookup)
logging.debug("worker threads: %s", worker_threads)

#activelist1 = Manager().dict()
#activelist2 = Manager().dict()
#activelist3 = Manager().dict()
#activelist4 = Manager().dict()

chosen_lib =  lib.pg_workload_lib



try:
   with open(args.myfile, "r") as read_file:
     settings = json.load(read_file)
   read_file.close();
except:
    logging.error('Problem reading file: %s ', args.myfile)

tag = settings['name']+str(settings['appnode'])+'-'+settings['host']

logging.debug('read file %s, found the following settings: %s', args.myfile, settings)

#set the driver if not in the settings file...
try: 
    settings['myclient'] 
except:
    settings['myclient'] = args.myclient

if (args.myclient != settings['myclient']):
    settings['myclient'] = args.myclient    
    
#fill in missing workload
try:
   settings['website_workload'] = settings['website_workload'] + 0
except:
   settings['website_workload'] =0
try:       
    settings['reporting_workload']= settings['reporting_workload'] + 0
except:
   settings['reporting_workload'] =0
try:
    settings['comments_workload']= settings['comments_workload'] + 0
except:
   settings['comments_workload'] =0    
try:
    settings['longtrans_workload']= settings['longtrans_workload'] + 0
except:
   settings['longtrans_workload'] =0
try:
    settings['special_workload'] = settings['special_workload'] + 0
except:
   settings['special_workload'] =0
try:
    settings['read_only_workload']= settings['read_only_workload'] + 0
except:
   settings['read_only_workload'] =0
try:
    settings['list_workload']= settings['list_workload'] + 0
except:
   settings['list_workload'] =0
try:
    settings['logging_workload']= settings['logging_workload'] + 0
except:
   settings['logging_workload'] =0  
  
MYDSN = "dbname=" + settings['database'] +" user="+ settings['username'] + " password=" + settings['password'] + " host=" + settings['host']

config = {
  'user': settings['username'],
  'password': settings['password'],
  'host': settings['host'],
  'database': settings['database'],
  'raise_on_warnings': False,
  'buffered':True
}

config_myclient = {
  'user': settings['username'],
  'passwd': settings['password'],
  'host': settings['host'],
  'db': settings['database']}

def reload_config(myfile):
    #function to check and reload the config file
    logging.debug("Inside Reload Config")
    try:
       with open(myfile, "r") as read_file:
         settings = json.load(read_file)
       read_file.close();
       try:
          settings['website_workload'] = settings['website_workload'] + 0
       except:
          settings['website_workload'] =0
       try:       
           settings['reporting_workload']= settings['reporting_workload'] + 0
       except:
          settings['reporting_workload'] =0
       try:
           settings['comments_workload']= settings['comments_workload'] + 0
       except:
          settings['comments_workload'] =0    
       try:
           settings['longtrans_workload']= settings['longtrans_workload'] + 0
       except:
          settings['longtrans_workload'] =0
       try:
           settings['special_workload'] = settings['special_workload'] + 0
       except:
          settings['special_workload'] =0
       try:
           settings['read_only_workload']= settings['read_only_workload'] + 0
       except:
          settings['read_only_workload'] =0
       try:
           settings['list_workload']= settings['list_workload'] + 0
       except:
          settings['list_workload'] =0
       try:
           settings['logging_workload']= settings['logging_workload'] + 0
       except:
          settings['logging_workload'] =0
       return settings
    except:
        logging.error('Problem reading file: %s ', myfile)
        return -1
        
def start_mysql() : 
     logging.info("Starting MySQL")
     global connect_string
     global chosen_lib 
     global mysql_driver
     if (args.myclient=='mysqlclient'):
        chosen_lib =  lib.mysql_workload_lib
        mysql_driver = 'mysqlclient'
        connect_string = config_myclient
        chosen_lib.mysql_driver = 'mysqlclient'
     elif (args.myclient=='pymysql'):
        chosen_lib =  lib.mysql_workload_lib
        mysql_driver = 'pymysql'
        connect_string = config_myclient
        chosen_lib.mysql_driver = 'pymysql'        
     else:
        chosen_lib =  lib.mysql_workload_lib
        mysql_driver = 'connector'
        chosen_lib.mysql_driver = 'connector'
        connect_string = config
     default_values = chosen_lib.load_db(connect_string)
     logging.debug("Finishing Start MySQL Func")
     return default_values
     
def start_pg() :
     logging.info("Starting PG") 
     global chosen_lib 
     chosen_lib =  lib.pg_workload_lib
     global connect_string
     connect_string = MYDSN
     default_values = chosen_lib.load_db(connect_string)
     logging.debug("Finishing Start MySQL Func")
     return default_values
     
def stop_mysql() : 
     logging.info('Stop MySQL')

def stop_pg() : 
     logging.info('Stop PG')   
     
def active_threads_list():
     return 'G:(' + str(len(thread_list[0])) + ') R: ('+ str(len(thread_list[1])) + ') IU: ('+ str(len(thread_list[2])) + ') LT: ('+ str(len(thread_list[3])) +') SP:(' + str(len(thread_list[4])) + ') RO: ('+ str(len(thread_list[5])) + ') JS: ('+ str(len(thread_list[6])) + ') MR: ('+ str(len(thread_list[7])) +') '

def spawn_app_nodes(count,wid):
     logging.debug("inside spawn_app_nodes")
     #worker_threads = ['chosen_lib.single_user_actions_v2','chosen_lib.report_user_actions','chosen_lib.insert_update_delete','chosen_lib.long_transactions']
     worker_desc = ['General Website','Reporting','Chat','Long Transactions','Special Workload','Read_only_workload','json','multi-row ro','logging heavy']
     logging.debug("WID: %s", wid)
     logging.debug("adding workers for: %s", worker_threads[wid])
    
     global activelist
     global thread_list
     global mysql_driver
     if count > 0:
        for x in range(count):
            process = multiprocessing.Process(target=eval(worker_threads[wid]), args=(connect_string,1000,0.1,0,0,default_values['list_actors'],default_values['list_titles'],default_values['list_ids'], default_values['ai_myids'],default_values,activelist[wid],timinglist,countlist,wid), daemon=True)
            process.start()
            activelist[wid][process.pid]=1
            logging.info('Started %s Workload as Pid %s', worker_desc[wid], process.pid)
            thread_list[wid].append(process)
            logging.info('%s Workload at count: %s', worker_desc[wid], str(len(thread_list[wid])) )   
            #adding in a small delay between threads to allow for workload to stagger better, other wise the start/stop times tend to align 
            time.sleep(0.2)                 
     if count < 0:
      for x in range(abs(count)):
          if (len(activelist[wid].keys()))> 0:
              process = thread_list[wid].pop()
              activelist[wid][process.pid]=0
              logging.info('%s Thread, Stopping Pid %s', worker_desc[wid], process.pid)
              logging.info('%s Workload at count: %s', worker_desc[wid], str(len(thread_list[wid])) )                    
     if (args.nopmm == 0 and count != 0 and args.time == -1) :
         os.system('pmm-admin annotate "' + worker_desc[wid] + ' Changed: ' + active_threads_list() + '" --tags "Benchmark, Workload Change,'+ tag +'"')
     logging.debug('%s Workload at count: %s', worker_desc[wid], str(len(thread_list[wid])) )
     logging.debug("Finishing call to spawn_app_nodes")
     
     
     
def spawn_special_nodes(count,wid,myfunction):
     logging.debug("inside spawn_special_nodes")
    
     global activelist
     global thread_list
     if count > 0:
        for x in range(count):
            func='chosen_lib.'+myfunction
            process = multiprocessing.Process(target=eval(func), args=(connect_string,1000,0.1,0,0,default_values['list_actors'],default_values['list_titles'],default_values['list_ids'], default_values['ai_myids'],default_values,activelist[wid],timinglist,countlist,wid), daemon=True)
            process.start()
            activelist[wid][process.pid]=1
            logging.info('Started %s Workload as Pid %s', myfunction, process.pid)
            thread_list[wid].append(process)
            logging.info('%s Workload at count: %s',myfunction, str(len(thread_list[wid])) )                    
     if count < 0:
      for x in range(abs(count)):
          if (len(activelist[wid].keys()))> 0:
              process = thread_list[wid].pop()
              activelist[wid][process.pid]=0
              logging.info('%s Thread, Stopping Pid %s', worker_desc[wid], process.pid)
              logging.info('%s Workload at count: %s', worker_desc[wid], str(len(thread_list[wid])) )                    
     if (args.nopmm == 0 and count != 0) :
         os.system('pmm-admin annotate "' + myfunction + ' Changed: ' + active_threads_list() + '" --tags "Benchmark, Workload Change,'+ tag +'"')
     logging.debug('%s Workload at count: %s', myfunction, str(len(thread_list[wid])) )
     logging.debug("Finishing call to spawn_special_nodes")
     
         
def event_spawn(eventid):
    return 0

def startup_workers():
    if args.special:
        spawn_special_nodes(args.threads, wid_lookup[args.function],user_function_list[args.function])
    else:
        spawn_app_nodes(settings['website_workload'], wid_lookup['website_workload'])
        spawn_app_nodes(settings['reporting_workload'], wid_lookup['reporting_workload'])
        spawn_app_nodes(settings['comments_workload'], wid_lookup['comments_workload'])
        spawn_app_nodes(settings['longtrans_workload'], wid_lookup['longtrans_workload'])
        spawn_app_nodes(settings['special_workload'], wid_lookup['special_workload'])
        spawn_app_nodes(settings['read_only_workload'], wid_lookup['read_only_workload'])
        spawn_app_nodes(settings['list_workload'], wid_lookup['list_workload'])
        spawn_app_nodes(settings['logging_workload'], wid_lookup['logging_workload'])
        if (args.nopmm == 0 ) :
            os.system('pmm-admin annotate "Start Up Workload: ' + args.myfile + ' : ' + active_threads_list() + '" --tags "Benchmark, Workload Change,'+ tag +'"')
def full_stop_workload():
    global activelist
    global thread_list
    for key in activelist[0].copy():
        activelist[0][key]=0
    thread_list[0] = []
    
    for key in activelist[1].copy():
        activelist[1][key]=0
    thread_list[1] = []
    
    for key in activelist[2].copy():
        activelist[2][key]=0
    thread_list[2] = []
    
    for key in activelist[3].copy():
        activelist[3][key]=0
    thread_list[3] = []
    
    for key in activelist[4].copy():
        activelist[4][key]=0
    thread_list[4] = []
    
    for key in activelist[5].copy():
        activelist[5][key]=0
    thread_list[5] = []
    
    for key in activelist[6].copy():
        activelist[6][key]=0
    thread_list[6] = []
    
    for key in activelist[7].copy():
        activelist[7][key]=0
    thread_list[7] = []
    
    if (args.nopmm == 0) :
        os.system('pmm-admin annotate "Full Stop Benchmark" --tags "Benchmark, Stop,'+ tag +'"')
    logging.info('Issued Deactivate Benchmark, Full Stop')
    logging.info('Final Time Check: %s' ,xt)
    logging.info('Final Active Counts: %s',countlist )
    logging.info('Final Timing Counts: %s',timinglist )
    res = [round((i / j),2) if j != 0 else 0 for i, j in zip(timinglist, countlist)]
    cntper = [round((i / xt),2) for i in countlist]
    logging.info('Final Time Per: %s',res )
    logging.info('Final Count PS: %s',cntper )


def signal_handler(sig, frame):
    logging.info('Starting Forced Shutdown!')
    full_stop_workload()
    time.sleep(5)
    sys.exit(0)
        
      
if settings['type'] == 'mysql' and settings['bench_active']==1:
        logging.debug("Setting up MySQL")                
        try:
            default_values = start_mysql()
            if (args.nopmm == 0) :
                os.system('pmm-admin annotate "Full MySQL Start" --tags "MySQL, Benchmark, Start-Stop,'+ tag +'"')
        except Exception as e:
            logging.error("error: %s", e)
            z = sys.exc_info()[0]
            logging.error("systems: %s",z )
               
if (settings['type'] == 'postgresql' or settings['type'] == 'pg') and settings['bench_active']==1:
        try:
            logging.debug("Setting up PG")
            default_values = start_pg()
            if (args.nopmm == 0) :
                os.system('pmm-admin annotate "Full PG Start" --tags "PostgreSQL, Benchmark, Start-Stop,'+ tag +'"')
        except Exception as e:
            logging.error("error: %s", e)
            z = sys.exc_info()[0]
            logging.error("systems: %s",z )
            
if settings['bench_active']==1:            
   startup_workers()
               
last_settings = settings;
start_time = time.perf_counter()

if (args.time > 0):
   logging.info('Will Terminate after %s' ,args.time)
   

try: 
 rpttime = 300
 while True:
    current_time=time.perf_counter()
    xt = current_time - start_time
    if (args.time > 0):
        if(args.time < 1200) :
          rpttime=60          
        if (xt > args.time ):
            logging.info('Reached time to shutdown')
            logging.info('FINAL: Active Counts: %s',countlist )
            logging.info('FINAL: Timing Counts: %s',timinglist )
            res = [round((i / j),2) if j != 0 else 0 for i, j in zip(timinglist, countlist)]
            cntper = [round((i / xt),2) for i in countlist]
            logging.info('FINAL: Time Per: %s',res )
            logging.info('FINAL: Count PS: %s',cntper )
            full_stop_workload()
            time.sleep(10)
            break
    
    if (round(xt) % rpttime == 0):
        logging.info('Time Check: %s' ,xt)
        logging.info('Active Counts: %s',countlist )
        logging.info('Timing Counts: %s',timinglist )
        res = [round((i / j),2) if j != 0 else 0 for i, j in zip(timinglist, countlist)]
        cntper = [round((i / xt),2) for i in countlist]
        logging.info('Time Per: %s',res )
        logging.info('Count PS: %s',cntper )
        
    try:
        new_settings = reload_config(args.myfile)
    except: 
        logging.warn('problem with settings file')
        
    if (last_settings != new_settings and args.special == 0):
        logging.info('Config Change Detected!')
        logging.info(active_threads_list())
        logging.info('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        logging.debug('last settings : %s', last_settings)
        logging.info('=======')
        logging.debug('new settings : %s', new_settings)
        logging.debug('++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++')
        try:
          if args.special:
              if settings['type'] == 'mysql':
                    try:
                        default_values = start_mysql()
                        start_time = time.perf_counter()
                        if (args.nopmm == 0) :
                            os.system('pmm-admin annotate "Full MySQL Start Workload:'+ args.function +'" --tags "MySQL, Benchmark, Start-Stop,'+ tag +'"')
                    except:
                        logging.error('Some Error')
               
              if settings['type'] == 'postgresql' or settings['type'] == 'pg':
                    try:
                        default_values = start_pg()
                        start_time = time.perf_counter()
                        if (args.nopmm == 0) :
                            os.system('pmm-admin annotate "Full PG Start Workload:'+ args.function +'" --tags "PostgreSQL, Benchmark, Start-Stop,'+ tag +'"')
                    except:
                        logging.error('Some Error')
              startup_workers()
              logging.info(active_threads_list())
          else:
           if (new_settings['bench_active'] == 1 and last_settings['bench_active']==0):
              if settings['type'] == 'mysql':
                    try:
                        default_values = start_mysql()
                        start_time = time.perf_counter()
                        if (args.nopmm == 0) :
                            os.system('pmm-admin annotate "Full MySQL Start Workload:'+ args.myfile +'" --tags "MySQL, Benchmark, Start-Stop,'+ tag +'"')
                    except:
                        logging.error('Some Error')
               
              if settings['type'] == 'postgresql' or settings['type'] == 'pg':
                    try:
                        default_values = start_pg()
                        start_time = time.perf_counter()
                        if (args.nopmm == 0) :
                            os.system('pmm-admin annotate "Full PG Start Workload:'+ args.myfile +'" --tags "PostgreSQL, Benchmark, Start-Stop,'+ tag +'"')
                    except:
                        logging.error('Some Error')
              startup_workers()
              logging.info(active_threads_list())
           else :
              if (new_settings['bench_active'] == 0 and last_settings['bench_active']==1):
                full_stop_workload();
              else :
                if (new_settings['website_workload'] - last_settings['website_workload'] != 0):
                    spawn_app_nodes(new_settings['website_workload'] - last_settings['website_workload'],wid_lookup['website_workload'])
                if (new_settings['reporting_workload'] - last_settings['reporting_workload'] != 0):
                    spawn_app_nodes(new_settings['reporting_workload'] - last_settings['reporting_workload'],wid_lookup['reporting_workload']) 
                if (new_settings['comments_workload'] - last_settings['comments_workload'] != 0):
                    spawn_app_nodes(new_settings['comments_workload'] - last_settings['comments_workload'],wid_lookup['comments_workload'])
                if (new_settings['longtrans_workload'] - last_settings['longtrans_workload'] != 0):
                    spawn_app_nodes(new_settings['longtrans_workload'] - last_settings['longtrans_workload'],wid_lookup['longtrans_workload'])   
                if (new_settings['list_workload'] - last_settings['list_workload'] != 0):
                    spawn_app_nodes(new_settings['list_workload'] - last_settings['list_workload'],wid_lookup['list_workload'])
                if (new_settings['special_workload'] - last_settings['special_workload'] != 0):
                    spawn_app_nodes(new_settings['special_workload'] - last_settings['special_workload'],wid_lookup['special_workload'])
                if (new_settings['read_only_workload'] - last_settings['read_only_workload'] != 0):
                    spawn_app_nodes(new_settings['read_only_workload'] - last_settings['read_only_workload'],wid_lookup['read_only_workload'])
                if (new_settings['logging_workload'] - last_settings['logging_workload'] != 0):
                    spawn_app_nodes(new_settings['logging_workload'] - last_settings['logging_workload'],wid_lookup['logging_workload'])


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
    logging.info('Final Time Check: %s' ,xt)
    logging.info('Final Active Counts: %s',countlist )
    logging.info('Final Timing Counts: %s',timinglist )
    res = [round((i / j),2) if j != 0 else 0 for i, j in zip(timinglist, countlist)]
    cntper = [round((i / xt),2) for i in countlist]
    logging.info('Final Time Per: %s',res )
    logging.info('Final Count PS: %s',cntper )
        

  
