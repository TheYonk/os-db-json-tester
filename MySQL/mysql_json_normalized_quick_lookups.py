import json 
import mysql.connector
import os 
import time 
import re
import random
import sys
import getopt

start_time = time.perf_counter()

config = {
  'user': 'movie_json_user',
  'password': 'Ch@nge_me_1st',
  'host': '127.0.0.1',
  'database': 'movie_json_test',
  'raise_on_warnings': True,
  'autocommit' : True
  
}

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor(buffered=True)
cursor2 = cnx.cursor(buffered=True)


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

load_actors = "select actor_name from movies_normalized_actors where actor_name != ''"
load_titles = "select title, imdb_id, year from movies_normalized_meta"

find_movies_by_actor = "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= %s and actor_name != ''"
find_movies_by_title = "select imdb_id, title, imdb_rating from movies_normalized_meta a where title = %s"


print("Loading Actors...")

cursor.execute(load_actors)

for actor_name in cursor:
        list_actors.append(actor_name);

print("Loading Titles & ID's...")

cursor.execute(load_titles)
for (movie_titles, imdb_id, mv_year) in cursor:
        list_tiles.append(movie_titles);
        list_ids.append(imdb_id);
        movie_years[imdb_id] = mv_year
               
start_time = time.perf_counter()
        
print("Starting Querying Data for "+ str(time_to_run) + " second ...")
while current_time < time_to_run : 
   current_time = time.perf_counter() - start_time
   search_actor = random.choice(list_actors)
   if debug == 1 :
     print("Actor: " + str(search_actor[0]))
   cursor.execute(find_movies_by_actor, search_actor)
   qry = qry + 1
   for (title, imdb_rating, actor_character ) in cursor:
       if debug == 1 :
         print(" -> Movie: " + title);
         
   search_title = random.choice(list_tiles)
   if debug == 1 :
     print("Title search: " + str(search_title))

   cursor.execute(find_movies_by_title, (search_title,))
   qry = qry + 1
   for (imdb_id,title, imdb_rating ) in cursor:
       if debug == 1 :
         print(" -> Movie: " + title);
       
  # print("Query: " + str(qry))

print("Ending Querying Data After "+ str(current_time) + " seconds ...  Finished " + str(qry) + " queries")
   