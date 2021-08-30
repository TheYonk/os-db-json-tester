# this script is to simulate adding comments and voting for a movie.  This is the primary script for transactions.

import json 
import psycopg2
import os 
import time 
import re
import random
import sys
import getopt

start_time = time.perf_counter()
MYDSN = "dbname=movie_json_test user=movie_json_user password=Change_me_1st! host=127.0.0.1";

cnx = psycopg2.connect(MYDSN)
cursor = cnx.cursor()
cursor2 = cnx.cursor()

if len(sys.argv) == 1:
     time_to_run = 300
else :
     print ("Run Time Set to: " + sys.argv[1])
     time_to_run = int(sys.argv[1])
     
# actors list stored here for random searches
list_actors=[]
#titles list here for random searches
list_tiles=[]
#list of ids
list_ids=[]
#list of ids
list_directors=[]
movie_years= dict()

current_time = 0
debug = 0
qry = 0;


batch_load_votes = " insert into movies_normalized_aggregate_ratings select a.ai_myid, avg(rating), max(upvotes), max(downvotes), max(imdb_rating) from movies_normalized_meta a, movies_normalized_user_comments b where a.ai_myid = b.ai_myid group by a.ai_myid on conflict (ai_myid) do update set user_rating = EXCLUDED.user_rating, up_votes = EXCLUDED.up_votes, down_votes = EXCLUDED.down_votes, imdb_rating = EXCLUDED.imdb_rating"
simulate_crappy_report = "select actor_name, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id  group by actor_name"
simulate_crappy_report2 = "select actor_name, round(year/10,0)*10 as year, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id  group by actor_name, round(year/10,0)*10  having count(*) > 5 order by actor_name"
# if we run longer then 1 hour, just do this batch every 15 minutes
if (time_to_run>3600):
    sleep_time = 900
else :
    #run 4 times per hour
    sleep_time = int(time_to_run/4)
    
               
start_time = time.perf_counter()
        
print("Starting Querying Data for "+ str(time_to_run) + " second ...")
while current_time < time_to_run : 
   current_time = time.perf_counter() - start_time

   cursor.execute(batch_load_votes)
   cnx.commit()
  
   cursor.execute(simulate_crappy_report)

   cursor.execute(simulate_crappy_report2)
   cnx.commit()    
   
   print("Batch Sleeping: " + str(sleep_time) + " seconds ")
   time.sleep(sleep_time)
   print("Batch Awoken from sleep")
   
   # print("Query: " + str(qry))
print("Ending Querying Data After "+ str(current_time) + " seconds ...  Finished " + str(qry) + " queries")
   