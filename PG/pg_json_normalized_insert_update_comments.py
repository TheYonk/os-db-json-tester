# this script is to simulate adding comments and voting for a movie.  This is the primary script for transactions.

import json 
import psycopg2
import os 
import time 
import re
import random
import sys
import getopt
import string

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
ai_myids=[]

current_time = 0
debug = 0
qry = 0;

load_ids = "select ai_myid, imdb_id, year from movies_normalized_meta"
comments_cleanup = "truncate movies_normalized_user_comments"

simulate_finding_a_movie_record = "select ai_myid, imdb_id, year, title, json_column from movies_normalized_meta where imdb_id = %s"
simulate_comment_on_movie = "insert into movies_normalized_user_comments (ai_myid,rating, comment ) values ( %s, %s, %s )"
simulate_updating_movie_record_with_vote = "update movies_normalized_meta set upvotes=upvotes+%s, downvotes=downvotes+%s where ai_myid = %s"

print("Loading Titles & ID's...")

#load our movies so we can randomlys select one later on.
cursor.execute(load_ids)
for (ai_myid, imdb_id, mv_year) in cursor:
        ai_myids.append(ai_myid);
        list_ids.append(imdb_id);
        movie_years[imdb_id] = mv_year

#to prevent the comments from getting too big we will truncate it at the start of every run.  
        
cursor.execute(comments_cleanup)
               
start_time = time.perf_counter()

letters = string.ascii_lowercase
        
print("Starting Querying Data for "+ str(time_to_run) + " second ...")
while current_time < time_to_run : 
   current_time = time.perf_counter() - start_time
   search_id = random.choice(list_ids)
   if debug == 1 :
     print("imdb id: " + str(search_id))
   cursor.execute(simulate_finding_a_movie_record, (search_id,))
   qry = qry + 1
   for ( ai_myid, imdb_id, year, title, json_column ) in cursor:
       if debug == 1 :
         print(" -> Movie: " + title);
   comment = "".join(random.choice(letters) for i in range (random.randrange(20,40)))
   uservote = random.randrange(0,10)
   
   cursor.execute(simulate_comment_on_movie,(ai_myid,uservote,comment))
   qry = qry + 1
   
   if uservote < 6 :
       downvote = 1
       upvote = 0
   else :
       downvote = 0
       upvote = 1
       
   cursor.execute(simulate_updating_movie_record_with_vote,(upvote,downvote,ai_myid))
   qry = qry + 1
   cnx.commit()    
  # print("Query: " + str(qry))

print("Ending Querying Data After "+ str(current_time) + " seconds ...  Finished " + str(qry) + " queries")
   