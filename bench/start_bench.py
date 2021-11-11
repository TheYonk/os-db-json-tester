# this script has a very simple purpose.  Loop through all Json in the config folder.  Spawn 1 worker/controller per json.  Exit.  Controller takes over the heavy lifting

import json 
import logging
import os

logging.basicConfig(level=logging.DEBUG)

valid = 0

directory = r'./configs/'
for myfile in os.scandir(directory):
    if (myfile.path.endswith(".json")):
        logging.info('found %s : starting validation', myfile.path )
    with open(myfile.path, "r") as read_file:
         try:
            data = json.load(read_file)
            valid = 1
         except: 
             logging.error('found %s does not contain a valid Json schema', myfile.path )            
             valid = 0
    if valid == 1:
        os.system('python controller.py --file=%s' % (myfile.path))    
        
             
    
 
 # I need to add schema validation here later to ensure it contains the elements I expect....
 # example:  https://pynative.com/python-json-validation/            