import json 
import psycopg2
import os 
import time 
import re
import random
import sys

start_time = time.perf_counter()
MYDSN = "dbname=movie_json_test user=movie_json_user password=Change_me_1st! host=127.0.0.1";

cnx = psycopg2.connect(MYDSN)
cnx.set_session(readonly=True, autocommit=True)

cursor = cnx.cursor()
cursor2 = cnx.cursor()


# actors list stored here for random searches
list_actors=[]
#titles list here for random searches
list_tiles=[]
#list of ids
list_ids=[]
#list of ids
list_directors=[]
movie_years= dict()

if len(sys.argv) == 1:
     time_to_run = 300
else :
     print ("Run Time Set to: " + sys.argv[1])
     time_to_run = int(sys.argv[1])
     
current_time = 0
debug = 0
qry = 0;

load_actors = "select actor_name from movies_normalized_actors where actor_name != ''"
load_titles = "select title, imdb_id, year from movies_normalized_meta"
load_ids = "select imdb_id from movies_normalized_meta"
load_directors = "select director from movies_normalized_director"

find_movies_by_actor = "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= %s and actor_name != ''"
find_movies_by_title = "select imdb_id, title, imdb_rating, json_column->>'$.overview' as overview from movies_normalized_meta a where title like %s"
find_movies_by_fuzzy_actor = "select imdb_id, title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name like %s"
find_actors_by_fuzzy_actor = "select actor_id,actor_name from movies_normalized_actors where actor_name like %s and actor_name != ''"

find_movies_by_id = "select imdb_id, title, imdb_rating, json_column->>'$.overview' as overview from movies_normalized_meta a where imdb_id like %s"
find_cast_by_id = "select title, imdb_rating, actor_name, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and imdb_id = %s"
find_movies_by_director = "select imdb_id, imdb_rating, title, director from movies_normalized_director a join movies_normalized_meta b on a.ai_myid = b.ai_myid where director = %s"
find_single_movies_by_director = "select imdb_id, imdb_rating, title, director from movies_normalized_director a join movies_normalized_meta b on a.ai_myid = b.ai_myid where director = %s limit 1"
find_movies_by_fuzzy_director = "select imdb_id, imdb_rating, title, director from movies_normalized_director a join movies_normalized_meta b on a.ai_myid = b.ai_myid where director like %s"
find_movies_and_directors_by_year = "select imdb_id, imdb_rating, title, director from movies_normalized_director a join movies_normalized_meta b on a.ai_myid = b.ai_myid where director like %s and year > %s and year < %s"

find_best_movies_by_year = "select imdb_id, imdb_rating, title from movies_normalized_meta where year = %s order by imdb_rating desc limit 10"
find_chinese_movies_single_year = "select title from movies_normalized_meta where country = 'China' and year = %s"
find_us_movies_single_year = "select title from movies_normalized_meta where country = 'USA' and year = %s"


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
 
print("Loading Directors...")

cursor.execute(load_directors)
        
for (director) in cursor:
        list_directors.append(director);


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
   if debug == 1 :
     print("Actor search: " + str(search_actor[0][0:6]))
     
   cursor.execute(find_actors_by_fuzzy_actor, (search_actor[0][0:6]+'%',))      
   qry = qry + 1     
   for (actor_id,actor_name ) in cursor:
       if debug == 1 :
           print(" -> -> Actors: " + actor_name + " : " + actor_id);

   cursor.execute(find_movies_by_fuzzy_actor, (search_actor[0][0:6]+'%',))      
   qry = qry + 1     
   for (imdb_id, imdb_rating, title, actor_character ) in cursor:
       if debug == 1 :
           print(" -> -> Movies for Actor: " + str(actor_name) + " : Movie:  " + str(title));
       cursor.execute(find_cast_by_id, (imdb_id,)) 
       junk = cursor.fetchall()     
       qry = qry + 1
   search_director = random.choice(list_directors)
   if debug == 1 :
     print("Director search: " + str(search_director[0]))
   cursor.execute(find_movies_by_director, (search_director[0],))      
   junk = cursor.fetchall()
   qry = qry + 1     
   
   search_director = random.choice(list_directors)     
   cursor.execute(find_movies_by_fuzzy_director, (search_director[0][0:6]+'%',))
   junk = cursor.fetchall()    
   qry = qry + 1     
   
   
   search_director = random.choice(list_directors)     
   cursor.execute(find_single_movies_by_director, (search_director[0],))
   junk = cursor.fetchall()    
   qry = qry + 1     
     
   for (imdb_id, imdb_rating, title,  director ) in cursor:
       if movie_years[imdb_id] is not None :
        minyr = movie_years[imdb_id] - 5
        maxyr = movie_years[imdb_id] + 5
       else:
         minyr = 1990
         maxyr = 2010
         
       cursor2.execute(find_movies_and_directors_by_year, (search_director[0],minyr,maxyr)) 
       junk = cursor.fetchall()     
       qry = qry + 1     
       
   searchyear = random.randrange(1945,2019)
   cursor.execute(find_best_movies_by_year, (searchyear,))
   junk = cursor.fetchall()   
   qry = qry + 1  
      
   searchyear = random.randrange(1945,2019)
   cursor.execute(find_chinese_movies_single_year, (searchyear,))
   junk = cursor.fetchall()
   qry = qry + 1             
 
   searchyear = random.randrange(1945,2019)
   cursor.execute(find_us_movies_single_year, (searchyear,))
   junk = cursor.fetchall()   
   qry = qry + 1          
       
  # print("Query: " + str(qry))

print("Ending Querying Data After "+ str(current_time) + " seconds ...  Finished " + str(qry) + " queries")
   