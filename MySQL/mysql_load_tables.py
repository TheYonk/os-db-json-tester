import json 
import mysql.connector
import os 
import time 
import re

start_time = time.perf_counter()
config = {
  'user': 'movie_json_user',
  'password': 'Ch@nge_me_1st',
  'host': '127.0.0.1',
  'database': 'movie_json_test',
  'raise_on_warnings': True
}

cnx = mysql.connector.connect(**config)
cursor = cnx.cursor()
actor_lookup=dict()
genre_lookup=dict()


insert_json = "replace into movies_json ( imdb_id, json_column ) values ( %s,  %s ) "
insert_json_hybrid = "replace into movies_json_hybrid ( imdb_id, title, imdb_rating, json_column ) values ( %s, %s, %s, %s )"

insert_norm_meta = "replace into movies_normalized_meta ( ai_myid, imdb_id, title, imdb_rating, year, country, json_column ) values ( %s, %s, %s, %s, %s, %s, %s  )"
insert_norm_actors = "insert  movies_normalized_actors ( actor_id, actor_name ) values ( %s, %s ) on duplicate key update actor_id=actor_id "
insert_norm_cast = "replace into movies_normalized_cast ( ai_actor_id, ai_myid, actor_character ) values ( %s, %s, %s )"
insert_director = "replace into movies_normalized_director ( ai_myid, director ) values ( %s, %s )"
insert_genre_tags = "insert into movies_normalized_genres_tags (genre) values ( %s ) on duplicate key update genre=genre"
insert_genre_movies = "replace into movies_normalized_genres (genre_id, ai_myid) values ( %s, %s ) "

truncate_genre_movies_generated = "truncate movies_json_generated";
insert_genre_movies_generated = "insert into movies_json_generated (json_column ) select json_column from  movies_json";


moviecount = 0
castcount = 0
actorcount = 0
directory = r'../meta/'
for myfile in os.scandir(directory):
  if (myfile.path.endswith(".json")):
    with open(myfile.path, "r") as read_file:
     data = json.load(read_file)
    moviecount +=1
    if (moviecount%1000 == 0) :
     #print("ID: " + data['imdb_id']);
     #print("Title: " + data['title']);
     #print("Director: " + data['director'][0]['name']);
     current_time = time.perf_counter() - start_time
     print("Processed " + str(moviecount) + " movies so far. " + str(actorcount) + " actors processed. " + str(castcount) + " characters added. Running Time: " + str(current_time)  )
    # note you have to send the json as a parameter so chars are escaped, and it must be a tuple otherwise it borks at you

    cursor.execute(insert_json,(data['imdb_id'], json.dumps(data)))

    #print("Last increment value: " + str(cursor.lastrowid))
    movie_ai = cursor.lastrowid

    #cursor.execute(insert_json_hybrid, (data['imdb_id'], data['title'], data['imdb_rating'], json.dumps(data)))


    #print("Last increment value: " + str(cursor.lastrowid))

   #movie_ai = cursor.lastrowid;
    my_title = data['title']
    t = re.findall("[0-9]{4}",my_title)
    #print (t)
    if len(t):
        my_year = t[-1]
    else :
        my_year = 0
       
    cursor.execute(insert_norm_meta, (movie_ai, data['imdb_id'], data['title'], data['imdb_rating'], my_year, data['country'], json.dumps(data)))

    if data['cast'] is not None : 
        for x in data['cast']:
         #print("cast: " + str(x) )
         castcount +=1
         cursor.execute(insert_norm_actors, (x['id'], x['name']))
         val =  x['id']
         if val not in  actor_lookup: 
            actor_lookup[val] = cursor.lastrowid;
            actorcount += 1
         cursor.execute(insert_norm_cast, (actor_lookup[x['id']], movie_ai, x['character'][0:499]))

    if data['director'] is not None :
        for x in data['director']:
             if x is not None :
               cursor.execute(insert_director, (movie_ai, x['name']))
    
    if data['genres'] is not None :
     for val in data['genres']:
       #print("Genres: " + val)
       if val not in genre_lookup: 
           cursor.execute(insert_genre_tags, (val,))
           new_id = cursor.lastrowid
           genre_lookup[val] = new_id
       cursor.execute(insert_genre_movies, (genre_lookup[val],movie_ai))
        
            
   
    # commit is needed as default for the connector is autocommit off
    cnx.commit()
    
cursor.execute(truncate_genre_movies_generated)
cursor.execute(insert_genre_movies_generated)
cnx.commit()

    
cursor.close()
cnx.close()
