import evdev
from evdev import InputDevice, categorize, ecodes
import asyncio
import pg_yonk_library
import mysql_yonk_library

import multiprocessing
from multiprocessing import Manager
import signal 
import logging
import errno
import os
import time

dev = evdev.InputDevice('/dev/input/event3')


#async def helper(device):
#    async for ev in device.async_read_loop():
#        print(repr(ev))
        
#loop = asyncio.get_event_loop()
#loop.run_until_complete(helper(device))

knobcounter = [0,0,0,0]
default_values = dict()
thread_list1 = []
thread_list2 = []
thread_list3 = []
thread_list4 = []

activelist1 = Manager().dict()
activelist2 = Manager().dict()
activelist3 = Manager().dict()
activelist4 = Manager().dict()



def wait_child(signum, frame):
  logging.info('receive SIGCHLD')
  try:
    while True:
      # -1  Represents any child process 
      # os.WNOHANG  Indicates if there are no available needs  wait  Exit status of the child process, immediately return non-blocking 
      cpid, status = os.waitpid(-1, os.WNOHANG)
      if cpid == 0:
        logging.info('no child process was immediately available')
        break
      exitcode = status >> 8
      logging.info('child process %s exit with exitcode %s', cpid, exitcode)
  except OSError as e:
    if e.errno == errno.ECHILD:
      logging.error('current process has no existing unwaited-for child processes.')
    else:
      raise
  logging.info('handle SIGCHLD end')

signal.signal(signal.SIGCHLD, wait_child)



MYDSN = "dbname=movie_json_test user=movie_json_user password=Change_me_1st! host=127.0.0.1";
config = {
  'user': 'movie_json_user',
  'password': 'Ch@nge_me_1st',
  'host': '127.0.0.1',
  'database': 'movie_json_test',
  'raise_on_warnings': False
}

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
     return 'G:(' + str(len(thread_list1)) + ') R: ('+ str(len(thread_list2)) + ') IU: ('+ str(len(thread_list3)) + ') LT: ('+ str(len(thread_list4)) +') '

try:
 os.system('pmm-admin annotate "Full Start" --tags "Benchmark, Start-Stop"')    
 for flipped in dev.active_keys():
    if flipped == 288:
        stop_mysql()
        print('Setting up PG')
        default_values = start_pg()
 if 'flipped' not in locals():
    stop_pg()
    print('Setting up MySQL')
    default_values = start_mysql()   
     
 for event in dev.read_loop():
       if event.type == 1:
         #print(str(event))
         if event.code == 288:
             if event.value == 1:
                print('Black Switch On!')
                print('Set to PostgreSQL')
                stop_mysql()
                default_values = start_pg()
                
             else:
                print('Black Switch off!') 
                stop_pg()
                print('Set to MySQL')
                default_values = start_mysql()
                
         if event.code == 289:
             if event.value == 1:
                print('Red Switch Flipped')
                print('Full Stop!')
                knobcounter[0] = 0
                for key in activelist1:
                    activelist1[key]=0
                thread_list1 = []
                
                knobcounter[1] = 0
                for key in activelist2:
                    activelist2[key]=0
                thread_list2 = []
                
                knobcounter[2] = 0
                for key in activelist3:
                    activelist3[key]=0
                thread_list3 = []
                
                knobcounter[3] = 0
                for key in activelist4:
                    activelist4[key]=0
                thread_list4 = []
                os.system('pmm-admin annotate "Full Stop Run" --tags "Benchmark, Workload Change"')
                
         if event.code == 290: 
             if event.value == 1:
                print('Blue Button')
             
         if event.code == 291:
             if event.value == 1:
                print('Yellow Button') 
         if event.code == 292: 
             if event.value == 1:
                print('Black Button #1')
                print('Running Vacuum/optimize')
                os.system('pmm-admin annotate "Running Vacuum/Optimize" --tags "Benchmark, Schema Change"')
                process = multiprocessing.Process(target=chosen_lib.run_vacuum, args=(connect_string,), daemon=True)
                process.start()
                
         if event.code == 293: 
             if event.value == 1:
                print('Toggle #1 down')
                print('Changeing PK to INT!')
                os.system('pmm-admin annotate "PK Changing to int" --tags "Benchmark, Schema Change"')
                process = multiprocessing.Process(target=chosen_lib.func_pk_int, args=(connect_string,), daemon=True)
                process.start()
             
         if event.code == 294: 
             if event.value == 1:
                print('Toggle #1 up')
                print('Changeing PK to INT!')
                os.system('pmm-admin annotate "PK Changing to int" --tags "Benchmark, Schema Change"')
                process = multiprocessing.Process(target=chosen_lib.func_pk_int, args=(connect_string,), daemon=True)
                process.start()
             
         if event.code == 295: 
             if event.value == 1:
                print('Toggle #2 down')
                print('Changeing PK to BigINT!')
                os.system('pmm-admin annotate "PK Changing to Bigint" --tags "Benchmark, Schema Change"')
                process = multiprocessing.Process(target=chosen_lib.func_pk_bigint, args=(connect_string,), daemon=True)
                process.start()
                
             
         if event.code == 296: 
             if event.value == 1:
                print('Toggle #2 up')
                print('Changeing PK to BigINT!')
                os.system('pmm-admin annotate "PK Changing to Bigint" --tags "Benchmark, Schema Change"')
                process = multiprocessing.Process(target=chosen_lib.func_pk_bigint, args=(connect_string,), daemon=True)
                process.start()
             
         if event.code == 297: 
             if event.value == 1:
                print('Black Button #2')
                print('Refreshing Materialized views #2')
                os.system('pmm-admin annotate "Running Vacuum" --tags "Benchmark, Schema Change"')
                process = multiprocessing.Process(target=chosen_lib.refresh_all_mat_views, args=(connect_string,), daemon=True)
                process.start()

         if event.code == 298: 
             if event.value == 1:
                print('Toggle #3 down')
                print('Changeing PK to varchar!')
                os.system('pmm-admin annotate "PK Changing to Varchar" --tags "Benchmark, Schema Change"')
                process = multiprocessing.Process(target=chosen_lib.func_pk_varchar, args=(connect_string,), daemon=True)
                process.start()
             
         if event.code == 299: 
             if event.value == 1:
                print('Toggle #3 up')
                print('Changeing PK to varchar!')
                os.system('pmm-admin annotate "PK Changing to Varchar" --tags "Benchmark, Schema Change"')
                process = multiprocessing.Process(target=chosen_lib.func_pk_varchar, args=(connect_string,), daemon=True)
                process.start()
                       
         if event.code == 300:
             if event.value == 1:
                print('Toggle #4 down')
              
         if event.code == 301: 
             if event.value == 1:
                print('Toggle #4 up')
             
         if event.code == 302: 
             if event.value == 1:
                print('Black Button #3')
             
         if event.code == 303: 
             if event.value == 1:
                print('Black Button #4')
             
         if event.code == 704: 
             if event.value == 1:
                print('Green Button #1')
                process = multiprocessing.Process(target=chosen_lib.func_create_drop_title_index, args=(connect_string,), daemon=True)
                process.start()
                os.system('pmm-admin annotate "add/dropped title index" --tags "Benchmark, Schema Change"')
                
         if event.code == 705: 
             if event.value == 1:
                print('Green Button #2')
                process = multiprocessing.Process(target=chosen_lib.func_create_drop_year_index, args=(connect_string,), daemon=True)
                process.start()
                os.system('pmm-admin annotate "add/dropped year index" --tags "Benchmark, Schema Change"')

                
         if event.code == 706: 
             if event.value == 1:
                print('Green Button #3')
                print('Running recreate json table')
                process = multiprocessing.Process(target=chosen_lib.make_copy_of_table, args=(connect_string,), daemon=True)
                process.start()
                print("proc pid: " , process.pid)
                
             
         if event.code == 707: 
             if event.value == 1:
                print('Green Button #4')
             
         if event.code == 708: 
             if event.value == 1:
                print('mystery 1')
                print('Reset Threads!')
                knobcounter[0] = 0
                for key in activelist1:
                    activelist1[key]=0
                thread_list1 = []
                os.system('pmm-admin annotate "General Workload Stopped: ' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                
         if event.code == 709: 
             if event.value == 1:
                print('mystery 2')
                print('Reset Threads!')
                knobcounter[1] = 0
                for key in activelist2:
                    activelist2[key]=0
                thread_list2 = []
                os.system('pmm-admin annotate "Reporting Workload Stopped: ' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                
         if event.code == 710: 
            if event.value == 1:
                print('mystery 3')
                print('Reset Threads!')
                knobcounter[2] = 0
                for key in activelist3:
                    activelist3[key]=0
                thread_list3 = []  
                          
         if event.code == 711: 
            if event.value == 1:
                print('mystery 4')
                print('Reset Threads!')
                knobcounter[3] = 0
                for key in activelist4:
                    activelist4[key]=0
                thread_list4 = [] 
                os.system('pmm-admin annotate "Long Transaction Workload Stopped' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                                         
         if event.code == 712: 
             if event.value == 1:
                knobcounter[0] = knobcounter[0] -1
                if knobcounter[0] > 0:
                   process = thread_list1.pop()
                   activelist1[process.pid]=0
                   print("proc pid: " , process.pid)
                print('Thread List Count: ' + str(len(thread_list1)) )
                os.system('pmm-admin annotate "General Workload Decreased: ' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                print('Knob #1 Down : ' + str(knobcounter[0]))
         if event.code == 713: 
             if event.value == 1:
                knobcounter[0] = knobcounter[0] +1
                #    threadlist.append(threading.Thread(target=single_user_actions_v2, args=(connect_string,args.time_to_run,args.sleep_timer,args.create_new_connection,args.create_new_connection_per_qry,list_actors,list_tiles,list_ids)))
                process = multiprocessing.Process(target=chosen_lib.single_user_actions_v2, args=(connect_string,1000,0.1,0,0,default_values['list_actors'],default_values['list_titles'],default_values['list_ids'], activelist1), daemon=True)
                process.start()
                activelist1[process.pid]=1
                print("proc pid: " , process.pid)
                
                thread_list1.append(process)
                print('Thread List Count: ' + str(len(thread_list1)) )
                os.system('pmm-admin annotate "General Workload Increased: ' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                
                print('Knob #1 Up : ' + str(knobcounter[0]))
         if event.code == 715: 
             if event.value == 1:
                knobcounter[1] = knobcounter[1] -1
                if knobcounter[1] > 0:
                   process = thread_list2.pop()
                   activelist2[process.pid]=0
                   print("proc pid: " , process.pid)
                print('Thread List Count: ' + str(len(thread_list2)) )
                os.system('pmm-admin annotate "Reporting Workload Decreased: ' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                
                print('Knob #2 Down : ' + str(knobcounter[1]))
                
         if event.code == 714: 
             if event.value == 1:
                knobcounter[1] = knobcounter[1] +1
                process = multiprocessing.Process(target=chosen_lib.report_user_actions, args=(connect_string,1000,0.25,0,0,default_values['list_actors'],default_values['list_titles'],default_values['list_ids'], activelist2), daemon=True)
                process.start()
                activelist2[process.pid]=1
                print("proc pid: " , process.pid)
                
                thread_list2.append(process)
                print('Thread List Count: ' + str(len(thread_list2)) )
                os.system('pmm-admin annotate "Reporting Workload Increased: ' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                            
                
                print('Knob #2 Up : ' + str(knobcounter[1]))
         if event.code == 717: 
             if event.value == 1:
                knobcounter[2] = knobcounter[2] -1
                if knobcounter[2] > 0:
                   process = thread_list3.pop()
                   activelist3[process.pid]=0
                   print("proc pid: " , process.pid)
                print('Thread List Count: ' + str(len(thread_list3)) )
                os.system('pmm-admin annotate "Insert/Update/Delete Workload Decreased: ' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                
                
                print('Knob #3 Down : ' + str(knobcounter[2]))             
         if event.code == 716: 
             if event.value == 1:
                knobcounter[2] = knobcounter[2] +1
                process = multiprocessing.Process(target=chosen_lib.insert_update_delete, args=(connect_string,1000,.5,0,0,default_values['list_actors'],default_values['list_titles'],default_values['ai_myids'], activelist3), daemon=True)
                process.start()
                activelist3[process.pid]=1
                print("proc pid: " , process.pid)
                thread_list3.append(process)
                print('Thread List Count: ' + str(len(thread_list3)) )
                os.system('pmm-admin annotate "Insert/Update/Delete Workload Decreased:' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                
                
                print('Knob #3 Up : ' + str(knobcounter[2]))     
                          
         if event.code == 719: 
             if event.value == 1:
                knobcounter[3] = knobcounter[3] - 1
                if knobcounter[3] > 0:
                   process = thread_list4.pop()
                   activelist4[process.pid]=0
                   print("proc pid: " , process.pid)
                print('Thread List Count: ' + str(len(thread_list4)) )
                os.system('pmm-admin annotate "Long Transaction Workload Decreased: ' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                
                print('Knob #4 down : ' + str(knobcounter[3]))
         if event.code == 718:  
             if event.value == 1:
                knobcounter[3] = knobcounter[3] +1
                process = multiprocessing.Process(target=chosen_lib.long_transactions, args=(connect_string,1000,1.5,0,0,default_values['list_actors'],default_values['list_titles'],default_values['list_ids'], activelist4), daemon=True)
                process.start()
                activelist4[process.pid]=1
                print("proc pid: " , process.pid)
                thread_list4.append(process)
                print('Thread List Count: ' + str(len(thread_list4)) )
                os.system('pmm-admin annotate "Long Transaction Workload Increased: ' + active_threads_list() + '" --tags "Benchmark, Workload Change"')
                
                
                print('Knob #4 Up : ' + str(knobcounter[3]))

 dev.close()

except KeyboardInterrupt:
    # quit
    print('Starting Shutdown!')
    dev.close()
    knobcounter[0] = 0
    for key in activelist1:
        activelist1[key]=0
    thread_list1 = []
    
    knobcounter[1] = 0
    for key in activelist2:
        activelist2[key]=0
    thread_list2 = []
    
    knobcounter[2] = 0
    for key in activelist3:
        activelist3[key]=0
    thread_list3 = []
    
    knobcounter[3] = 0
    for key in activelist4:
        activelist4[key]=0
    thread_list4 = []
    print('waiting 10 seconds to shutdown!')
    os.system('pmm-admin annotate "Full Shutdown" --tags "Benchmark, Start-Stop"')
    time.sleep(10)
    print('shutdown over!')
    
