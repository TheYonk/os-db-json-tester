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
parser.add_argument('-w', '--webusers', dest='webusers', type=int, default=0, help='# of web users')
parser.add_argument('-r', '--reportusers', dest='rptusers', type=int, default=0, help='# of reporting users')
parser.add_argument('-c', '--chatusers', dest='chatusers', type=int, default=0, help='# of chat users')
parser.add_argument('-l', '--longtrans', dest='longtrans', type=int, default=0, help='# of long running users')
parser.add_argument('-ro', '--readonly', dest='readonly', type=int, default=0, help='# of readonly users')
parser.add_argument('-li', '--listworkload', dest='listw', type=int, default=0, help='# of users returning multi-row queries')
parser.add_argument('-sp', '--special', dest='special', type=int, default=0, help='# of users runing some experimental special stuff')

parser.add_argument('-a', '--active', dest='active', action="store_true", default=0, help='database we are targeting')

args = parser.parse_args()
print(args)

with open(config_dir+args.myfile+'.json', "r") as read_file:
     settings_full = json.load(read_file)


if args.active:
  bench_active = 1
else: 
  bench_active = 0
  
for settings in settings_full['databases']:
    new_config = {
                "name" : args.myfile,
                "desc" : settings['dbtype'] + ' Movie Database Test for: ' + args.myfile,
             	"appnode" : settings['appnode'],	
             	"host" :  settings['dbhost'],	
             	"username" : settings['dbusername'],	
             	"password" : settings['dbpassword'],	
             	"database" : settings['database'],	
             	"bench_active" : bench_active,
                 "type" : settings['dbtype'],	
                 "website_workload" : args.webusers,
                 "reporting_workload" : args.rptusers,
                 "comments_workload" : args.chatusers,
                 "longtrans_workload" : args.longtrans,
                 "read_only_workload" : args.readonly,
                 "list_workload" : args.listw,
                 "special_workload" : args.special,
                 "title_idx" : 1,
             	 "year_idx" : 1
             }
     
    json_object = json.dumps(new_config, indent = 4) 
    outfile = './tmp/'+ args.myfile + str(settings['appnode']) + '.json'
    with open(outfile, 'w') as f:
        f.write(json_object)
    t = 'scp '+ outfile + ' ' + settings['hostuser']+ '@' + settings['host'] + ':' + settings['config_dir']
    os.system(t)
    logging.info(t)
