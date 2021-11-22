import json 
import asyncio
#import pg_yonk_library
#import mysql_yonk_library
import argparse
import multiprocessing
from multiprocessing import Manager
import signal 
import logging
import errno
import os
import time

logging.basicConfig(level=logging.DEBUG)
config_dir = './config/'

parser = argparse.ArgumentParser()
parser.add_argument('-n', '--name', dest='myfile', type=str, default="", help='the unique name of the app to use, defined in the configs')
parser.add_argument('-e', '--event', dest='myevent', type=str, default="", help='the name of the event we want queued')

#parser.add_argument('-b', '--backup', dest='backup', action="store_true",, default=0, help='Queue Backups')
#parser.add_argument('-v', '--vacuum', dest='vacuum', action="store_true",, default=0, help='Queue Vacuum or Optimize')
#parser.add_argument('-fv', '--fullvacuum', dest='fullvacuum', action="store_true",, default=0, help='Queue Vacuum or Optimize')
#parser.add_argument('-pb', '--pk-bigint', dest='pkbig', action="store_true",, default=0, help='# of long running users')
#parser.add_argument('-pi', '--pk-int', dest='pkint', action="store_true", default=0, help='database we are targeting')
#parser.add_argument('-pv', '--pk-varchar', dest='pkvar', action="store_true", default=0, help='database we are targeting')
#parser.add_argument('-cti', '--create_title_idx', dest='c_title_idx', action="store_true", default=0, help='database we are targeting')
#parser.add_argument('-dti', '--drop_title_idx', dest='d_title_idx', action="store_true", default=0, help='database we are targeting')
#parser.add_argument('-cyi', '--create_year_idx', dest='c_year_idx', action="store_true", default=0, help='database we are targeting')
#parser.add_argument('-dyi', '--drop_year_idx', dest='d_year_idx', action="store_true", default=0, help='database we are targeting')
#parser.add_argument('-dl', '--data-load-json', dest='dataload', action="store_true", default=0, help='database we are targeting')
#parser.add_argument('-rv', '--refresh-mviews', dest='refreshmv', action="store_true", default=0, help='database we are targeting')

args = parser.parse_args()
#print(args)

with open(config_dir+args.myfile+'.json', "r") as read_file:
     settings_full = json.load(read_file)

logging.info('Event %s requested, starting validation', args.myevent)

with open('bench_setup.json', "r") as setup_file:
    try:
       setup = json.load(setup_file)
       valid = 1
    except: 
        logging.error('found %s does not contain a valid Json schema', setup_file )            
        valid = 0
        
for settings in settings_full['databases']:

    if args.myevent in setup[settings['dbtype']]['events'].keys():
        logging.info('Event valid, sending request')
    else: 	
        logging.error('Event is not valid, sending request')
        logging.error('Valid Events for this system are: %s ', setup[settings['dbtype']]['events'].keys())
        exit();
    
    
    new_event = {
                'name' : args.myfile,
                'desc' : settings['dbtype'] + ' Movie Database Test for: ' + args.myfile,
             	"event" : args.myevent,	
                "type" : settings['dbtype'],	
             	"host" :  settings['dbhost'],	
             	"username" : settings['dbusername'],	
             	"password" : settings['dbpassword'],	
             	"database" : settings['database'],	
             }

    mytime = int(time.time())     
    json_object = json.dumps(new_event, indent = 4) 
    outfile = './tmp/'+ args.myfile + '.' + args.myevent + '.' + str(mytime) + '.json'
    with open(outfile, 'w') as f:
        f.write(json_object)

    os.system('scp '+ outfile + ' ' + settings['hostuser']+ '@' + settings['dbhost_shell'] + ':' + settings['event_dir'] )
    logging.debug('Command Running: %s', 'scp '+ outfile + ' ' + settings['hostuser']+ '@' + settings['dbhost_shell'] + ':' + settings['event_dir'] )
    os.system('rm '+ outfile)
