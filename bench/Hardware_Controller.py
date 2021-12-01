import json 
import argparse
#import asyncio
#import pg_yonk_library
#import mysql_yonk_library
import evdev
#import multiprocessing
#from multiprocessing import Manager
import signal 
import logging
import errno
import os
import time
import sys
import serial

mydatabase = 'off'
#to add to/use a serial device you need to add yourself to the dial out group! in linux
#i.e.  sudo usermod -a -G dialout user

import serial.tools.list_ports

def print_ports():
  ports = list(serial.tools.list_ports.comports())
  for p in ports:
    print(p)
    
logging.basicConfig(level=logging.DEBUG)
config_dir = './config/'

parser = argparse.ArgumentParser()
parser.add_argument('-i', '--interface', dest='interface', type=str, default="controller", help='the interface to use, either controller for HID, or serial for Serial interface')
parser.add_argument('-e', '--eventfile', dest='eventfile', type=str, default="", help='the name of the hid event')
parser.add_argument('-c', '--comport', dest='comport', type=str, default="", help='the name of the com port')
parser.add_argument('-mw', '--max_web', dest='max_web', type=int, default="30", help='max threads for web workload')
parser.add_argument('-mr', '--max_report', dest='max_report', type=int, default="15", help='max threads for reporting workload')
parser.add_argument('-mc', '--max_chat', dest='max_chat', type=int, default="30", help='max threads for chat workload')
parser.add_argument('-ml', '--max_long', dest='max_long', type=int, default="10", help='max threads for long workload')

args = parser.parse_args()

def print_devices():
    devices = [evdev.InputDevice(path) for path in evdev.list_devices()]
    for device in devices:
      logging.error("Posible device: %s, %s, %s ", device.path, device.name, device.phys)
      device.close()


if args.interface not in ["controller","serial"]:
   logging.error("interface not found, either use controller or serial")
   exit()
   
if args.interface == 'controller':
    try:
       dev = evdev.InputDevice(args.eventfile)
    except Exception as e:
       logging.error("error: %s", e)
       z = sys.exc_info()[0]
       logging.error("systems: %s",z )
       logging.error("failed to connect to device: %s",args.eventfile)
       print_devices()
       exit()

if args.interface == 'serial':
   try :
      arduino = serial.Serial(port=args.comport, baudrate=115200, timeout=.5)
      arduino.setDTR(False)
      x = arduino.readline().decode().strip()
      time.sleep(1)
      arduino.flushInput()
      arduino.setDTR(True)
      
   except Exception as e:
      logging.error("Missing Valid Com Port:")
      logging.error("error: %s", e)
      z = sys.exc_info()[0]
      logging.error("systems: %s",z )
      print_ports()
      exit()

# event 288 is a tough one, it is currently set here to match the name in the config files...

eventlist = {
     "blue_button" : {"code": 290, "on":"turn_on", "off":"", "type": "work" },
     "yellow_button" : {"code": 291, "on":"turn_off", "off":"", "type": "work"  },
     "black_swtich" : { "code" : 288, "on" : "pg", "off" : "mysql", "type": "type" },
     "red_switch" : { "code" : 289, "on" : "shutdown", "off" : "", "type": "reset" },
     "green_button1" : { "code" : 704, "on" : "change_title_idx", "off" : "", "type": "event" },
     "green_button2" : { "code" : 705, "on" : "change_year_idx", "off" : "", "type": "event" },
     "green_button3" : { "code" : 706, "on" : "create_json_table", "off" : "", "type": "event" },
     "green_button4" : { "code" : 707, "on" : "", "off" : "", "type": "" },
     "black_button1" : { "code" : 292, "on" : "vacuum", "off" : "", "type": "event" },
     "black_button2" : { "code" : 297, "on" : "refresh_mat_view", "off" : "", "type": "event" },
     "black_button3" : { "code" : 302, "on" : "", "off" : "", "type": "" },
     "black_button4" : { "code" : 303, "on" : "", "off" : "", "type": "" },
     "toggle1up" : { "code" : 294, "on" : "change_pk_to_int", "off" : "", "type": "event" },
     "toggle2up" : { "code" : 296, "on" : "change_pk_to_bigint", "off" : "", "type": "event" },
     "toggle3up" : { "code" : 299, "on" : "change_pk_to_varchar", "off" : "", "type": "event" },
     "toggle4up" : { "code" : 301, "on" : "", "off" : "", "type": "" },
     "toggle1down" : { "code" : 293, "on" : "change_pk_to_int", "off" : "", "type": "event" },
     "toggle2down" : { "code" : 295, "on" : "change_pk_to_bigint", "off" : "", "type": "event" },
     "toggle3down" : { "code" : 298, "on" : "change_pk_to_varchar", "off" : "", "type": "event" },
     "toggle4down" : { "code" : 300, "on" : "", "off" : "", "type": "" },               
     "rotary1up" : { "code" : 713, "on" : "web_up", "off" : "", "type": "work" },
     "rotary2up" : { "code" : 714, "on" : "report_up", "off" : "", "type": "work" },
     "rotary3up" : { "code" : 716, "on" : "chat_up", "off" : "", "type": "work" },
     "rotary4up" : { "code" : 718, "on" : "long_up", "off" : "", "type": "work" },  
     "rotary1down" : { "code" : 712, "on" : "web_down", "off" : "", "type": "work" },
     "rotary2down" : { "code" : 715, "on" : "report_down", "off" : "", "type": "work" },
     "rotary3down" : { "code" : 717, "on" : "chat_down", "off" : "", "type": "work" },
     "rotary4down" : { "code" : 719, "on" : "long_down", "off" : "", "type": "work" },      
     "rotary1button" : { "code" : 708, "on" : "web_0", "off" : "", "type": "work" },
     "rotary2button" : { "code" : 709, "on" : "report_0", "off" : "", "type": "work" },
     "rotary3button" : { "code" : 710, "on" : "chat_0", "off" : "", "type": "work" },
     "rotary4button" : { "code" : 711, "on" : "long_0", "off" : "", "type": "work" }   
}

event_code_lookup = dict()

for myval in eventlist:
    event_code_lookup[eventlist[myval]['code']] = { "on" :  eventlist[myval]['on'], "off" :  eventlist[myval]['off'], "button": myval, "type" :  eventlist[myval]['type']}
    
print(event_code_lookup)

myfile = "./config/startup_settings.json"
settings = ""



try:
   with open(myfile, "r") as read_file:
     settings = json.load(read_file)
   read_file.close();
except Exception as e:
      logging.error("error: %s", e)
      z = sys.exc_info()[0]
      logging.error("systems: %s",z )
      logging.error("failed to reaf file: %s",myfile)




# hardcoding 288 as a the DB selector for now... should be better way in the future...


def workload_change(x,name) :
    logging.debug("workload change func : x : %s , name: %s", x, name) 
    if x == "turn_on":
        settings['databases'][name]['bench_active'] = 1   
    if x == "turn_off":
        settings['databases'][name]['bench_active'] = 0
    if x == "web_up":
        settings['databases'][name]['website_workload'] = settings['databases'][name]['website_workload'] + 1
    if x == "report_up":
        settings['databases'][name]['reporting_workload'] = settings['databases'][name]['reporting_workload'] + 1
    if x == "chat_up":
        settings['databases'][name]['comments_workload'] = settings['databases'][name]['comments_workload'] + 1
    if x == "long_up":
        settings['databases'][name]['longtrans_workload'] = settings['databases'][name]['longtrans_workload'] + 1
    if x == "web_down":
        settings['databases'][name]['website_workload'] = settings['databases'][name]['website_workload'] - 1
    if x == "report_down":
        settings['databases'][name]['reporting_workload'] = settings['databases'][name]['reporting_workload'] - 1
    if x == "chat_down":
        settings['databases'][name]['comments_workload'] = settings['databases'][name]['comments_workload'] - 1    
    if x == "long_down":
        settings['databases'][name]['longtrans_workload'] = settings['databases'][name]['longtrans_workload'] - 1
    if x == "web_0" or settings['databases'][name]['website_workload'] <0:
        settings['databases'][name]['website_workload'] = 0
    if x == "report_0" or settings['databases'][name]['reporting_workload'] <0:
        settings['databases'][name]['reporting_workload'] = 0
    if x == "chat_0" or settings['databases'][name]['comments_workload'] <0:
        settings['databases'][name]['comments_workload'] = 0
    if x == "long_0" or settings['databases'][name]['longtrans_workload'] <0:
        settings['databases'][name]['longtrans_workload'] = 0  
                     

def build_config_command(mydatabase):
    if settings['databases'][mydatabase]['bench_active'] == 1:
       active_flag = " -a"
    else :
       active_flag = " "
    x = 'python3 config_generator.py -n '+ mydatabase +' -w ' + str(settings['databases'][mydatabase]['website_workload'])  +' -r ' + str(settings['databases'][mydatabase]['reporting_workload']) +' -c ' + str(settings['databases'][mydatabase]['comments_workload'])  + ' -l ' + str(settings['databases'][mydatabase]['longtrans_workload']) + active_flag + ' & '
    return x
    
def reset():
     for x in settings['databases'].keys():
         settings['databases'][x]['bench_active'] = 0

         
if args.interface == 'controller': 
 logging.debug("Active Keys: %s", dev.active_keys())
 logging.debug("Active Keys: %s", dev)
    
 if 288 in dev.active_keys():
        mydatabase = event_code_lookup[288]['on']
        print('Setting up PG')
 else:
        mydatabase = event_code_lookup[288]['off']
        print('Setting up MySQL')
 try:      
  for event in dev.read_loop():
      if event.type == 1:
         if event.value == 1:
            logging.debug("Event Type: %s , Event Code: %s , event Value: %s , active keys: %s", event.type, event.code, event.value, dev.active_keys()) 
            logging.info("Button Pressed: %s , mapped to action:  %s", event_code_lookup[event.code]['button'], event_code_lookup[event.code]['on'] )
            if event.code == 288:
               mydatabase = event_code_lookup[288]['on']
            if event_code_lookup[event.code]['type'] == "event":
               #os.system('python3 event_generator.py -n '+ mydatabase +' -e ' + event_code_lookup[event.code]['on'] )
                t = 'python3 event_generator.py -n '+ mydatabase +' -e ' + event_code_lookup[event.code]['on']
                logging.debug("Event Generator Command : %s", t)
                os.system(t)
            if event_code_lookup[event.code]['type'] == "work":
               if event_code_lookup[event.code]['on']:
                  workload_change(event_code_lookup[event.code]['on'],mydatabase)   
                  t = build_config_command(mydatabase)
                  logging.debug("Config Generator Command : %s", t)
                  os.system(t);
            if event_code_lookup[event.code]['type'] == "reset":
                reset()
                workload_change(event_code_lookup[event.code]['on'],mydatabase)  
                t = build_config_command(mydatabase) 
                logging.debug("Workload Shutoff Command : %s", t)
                os.system(t);
         if event.value == 0:
            if event_code_lookup[event.code]['off']: 
              logging.debug("Event Type: %s , Event Code: %s , event Value: %s , active keys: %s", event.type, event.code, event.value, dev.active_keys()) 
              logging.info("Button Pressed: %s , mapped to action:  %s", event_code_lookup[event.code]['button'], event_code_lookup[event.code]['off'] )
              if event.code == 288:
                 mydatabase = event_code_lookup[288]['off']
             
  dev.close()
  exit()    

 except KeyboardInterrupt:
    # quit
    print('Starting Shutdown!')
    dev.close()
    print('waiting 5 seconds to shutdown!')
    os.system('pmm-admin annotate "Full Shutdown" --tags "Benchmark, Start-Stop"')
    time.sleep(5)
    print('shutdown over!')
 except Exception as e:
      logging.error("error: %s", e)
      z = sys.exc_info()[0]
      logging.error("systems: %s",z )
      logging.error("failed to reaf file: %s",myfile)
    
if args.interface == 'serial':
 try:      
     time.sleep(10)
     arduino.flushInput()
     while (True):
         x = arduino.readline().decode().strip()
         #decoded_bytes = float(x[0:len(x)-2].decode("utf-8"))
         if(x != ""):
             #print(x)
             mystuff = x.split(",")
             #logging.debug("Recieved: %s", mystuff)
             logging.debug("type: %s", mystuff[0])
             logging.debug("command: %s", mystuff[2])
             if (mystuff[0]=='type'):
                 mydatabase = mystuff[2]
             if (mystuff[0]=='reset'):
                 mydatabase = 'off'            
             if mydatabase != 'off' and mydatabase != 'mongodb':
               if (mystuff[0]=='event'):
                 if ( mystuff[2] != 'Stop_All' ) :
                     t = 'python3 event_generator.py -n '+ mydatabase +' -e ' + mystuff[2] + ' & '
                     logging.debug("Event Generator Command : %s", t)
                     os.system(t)
               if (mystuff[0]=='workload'):
                 logging.debug("workload threads: %s", mystuff[3])
                 if  (mystuff[2] == 'change_lock_workload'):
                    threads = int(round(float(mystuff[3])/100 * int(args.max_long)))
                    settings['databases'][mydatabase]['longtrans_workload'] = threads  
                    t = build_config_command(mydatabase)
                    logging.debug("Config Generator Command : %s", t)
                    os.system(t);     
                 if  (mystuff[2] == 'change_chat_workload'):
                    threads = int(round(float(mystuff[3])/100 * int(args.max_chat))) 
                    settings['databases'][mydatabase]['comments_workload']  = threads   
                    t = build_config_command(mydatabase)
                    logging.debug("Config Generator Command : %s", t)  
                    os.system(t);       
                 if  (mystuff[2] == 'change_reporting_workload'):
                    threads = int(round(float(mystuff[3])/100 * int(args.max_report)))  
                    settings['databases'][mydatabase]['reporting_workload'] = threads   
                    t = build_config_command(mydatabase)
                    logging.debug("Config Generator Command : %s", t)
                    os.system(t);     
                 if  (mystuff[2] == 'change_website_workload'):
                    threads = int(round(float(mystuff[3])/100 * int(args.max_web)))   
                    settings['databases'][mydatabase]['website_workload']  = threads   
                    logging.debug("Current Settings: %s", settings['databases'][mydatabase])
                    t = build_config_command(mydatabase)
                    os.system(t);     
                    logging.debug("Config Generator Command : %s", t)
             
         #time.sleep(1)
         
 except KeyboardInterrupt:
    # quit
    print('Starting Shutdown!')
    
    if args.interface == 'serial': 
       arduino.close();
    else :
        dev.close();
    print('waiting 5 seconds to shutdown!')
    os.system('pmm-admin annotate "Full Shutdown" --tags "Benchmark, Start-Stop"')
    time.sleep(5)
    print('shutdown over!')
    
 except Exception as e:
      logging.error("error: %s", e)
      z = sys.exc_info()[0]
      logging.error("systems: %s",z )
