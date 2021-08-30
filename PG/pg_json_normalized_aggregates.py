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


find_best_movies_by_year = "select imdb_id, imdb_rating, title from movies_normalized_meta where year = %s order by imdb_rating desc"
find_movie_ratings_count_for_years = "select count(*), imdb_rating from movies_normalized_meta where year > %s  and year < %s group by imdb_rating"
find_movies_by_years = "select count(*), year from movies_normalized_meta group by year order by year desc"

find_movies_and_directors_years_fuzzy = "select year, director, count(*), avg(imdb_rating) from movies_normalized_director a join movies_normalized_meta b on a.ai_myid = b.ai_myid where director like %s group by year, director "
find_movies_and_directors_by_years = "select year, director, count(*), avg(imdb_rating) from movies_normalized_director a join movies_normalized_meta b on a.ai_myid = b.ai_myid where  year > %s and year < %s  group by year, director "

actor_movie_count_by_year = "select actor_name, year, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name = %s group by year, actor_name"
actor_movie_count_by_year_fuzzy = "select actor_name, year, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name = %s group by year, actor_name"

actor_movie_count_by_year_range = "select actor_name, year, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and year between %s and %s group by year, actor_name"

find_country_movies_single_year = "select country, count(*) from movies_normalized_meta where year = %s group by country"
find_country_movies_range_year = "select year, country, count(*) from movies_normalized_meta where year between %s and %s  group by year, country"



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
   cursor.execute(actor_movie_count_by_year, search_actor)
   qry = qry + 1
   junk = cursor.fetchall()
     
   cursor.execute(actor_movie_count_by_year_fuzzy, (search_actor[0][0:6]+'%',))      
   qry = qry + 1     
   junk = cursor.fetchall()
   
   searchyear = random.randrange(1945,2019)
   cursor.execute( actor_movie_count_by_year_range, (searchyear, searchyear+5))
   qry = qry + 1     
   junk = cursor.fetchall()
   
   
   search_director = random.choice(list_directors)     
   cursor.execute(find_movies_and_directors_years_fuzzy, (search_director[0][0:6]+'%',)) 
   qry = qry + 1     
   junk = cursor.fetchall()
   
   searchyear = random.randrange(1945,2019)
   cursor.execute( find_movies_and_directors_by_years, (searchyear, searchyear+5))
   qry = qry + 1     
   junk = cursor.fetchall()
        
 
   searchyear = random.randrange(1945,2019)
   cursor.execute( find_movie_ratings_count_for_years, (searchyear, searchyear+5))
   qry = qry + 1   
   junk = cursor.fetchall()
   
   cursor.execute( find_movies_by_years)
   qry = qry + 1
   junk = cursor.fetchall()
    
   searchyear = random.randrange(1945,2019) 
   cursor.execute( find_best_movies_by_year, (searchyear,))
   qry = qry + 1
   junk = cursor.fetchall()

   searchyear = random.randrange(1945,2019) 
   cursor.execute( find_country_movies_single_year, (searchyear,))
   qry = qry + 1
   junk = cursor.fetchall()
         
   searchyear = random.randrange(1945,2019)
   cursor.execute( find_country_movies_range_year, (searchyear, searchyear+5))
   qry = qry + 1   
   junk = cursor.fetchall()
         
  # print("Query: " + str(qry))

print("Ending Querying Data After "+ str(current_time) + " seconds ...  Finished " + str(qry) + " queries")
   