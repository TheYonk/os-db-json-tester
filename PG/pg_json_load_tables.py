import json 
import psycopg2
import os 
import time 
import re

start_time = time.perf_counter()
MYDSN = "dbname=movie_json_test user=movie_json_user password=Change_me_1st! host=127.0.0.1";

cnx = psycopg2.connect(MYDSN)
cursor = cnx.cursor()
actor_lookup=dict()
genre_lookup=dict()


insert_json = "insert into movies_json ( ai_myid, imdb_id, json_column ) values ( DEFAULT, %s,  %s ) on conflict (imdb_id) do update set json_column=EXCLUDED.json_column returning ai_myid"

truncate_norm_meta= "truncate movies_normalized_meta";
truncate_norm_actors = "truncate movies_normalized_actors";
truncate_norm_cast = "truncate movies_normalized_cast";
truncate_norm_director = "truncate movies_normalized_director";
truncate_norm_genres_tags = "truncate movies_normalized_genres_tags";
truncate_norm_genres = "truncate movies_normalized_genres";
reset_sequence1 = "alter SEQUENCE movies_json_ai_myid_seq restart with 1"
reset_sequence2 = "alter SEQUENCE movies_json_generated_ai_myid_seq restart with 1"
reset_sequence3 = "alter SEQUENCE movies_jsonb_ai_myid_seq restart with 1"
reset_sequence4 = "alter SEQUENCE movies_normalized_actors_ai_actor_id_seq restart with 1"
reset_sequence5 = "alter SEQUENCE movies_normalized_cast_inc_id_seq restart with 1"
reset_sequence6 = "alter SEQUENCE movies_normalized_director_director_id_seq restart with 1"
reset_sequence7 = "alter SEQUENCE movies_normalized_genres_tags_genre_id_seq restart with 1"
reset_sequence8 = "alter SEQUENCE movies_normalized_meta_ai_myid_seq restart with 1"

insert_norm_meta = "insert into movies_normalized_meta ( ai_myid, imdb_id, title, imdb_rating, year, country, json_column ) values ( %s, %s, %s, %s, %s, %s, %s  ) returning ai_myid"
insert_norm_actors = "insert into movies_normalized_actors ( ai_actor_id, actor_id, actor_name ) values (DEFAULT,  %s, %s ) on conflict (actor_id) do nothing returning ai_actor_id"
insert_norm_cast = "insert into movies_normalized_cast ( ai_actor_id, ai_myid, actor_character ) values ( %s, %s, %s ) returning inc_id"
insert_director = "insert into movies_normalized_director ( ai_myid, director ) values ( %s, %s ) returning director_id"
insert_genre_tags = "insert into movies_normalized_genres_tags (genre) values ( %s ) on conflict  (genre) do nothing returning genre_id"
insert_genre_movies = "insert into movies_normalized_genres (genre_id, ai_myid) values ( %s, %s ) on conflict on constraint movies_normalized_genres_pkey do nothing"

truncate_genre_movies_generated = "truncate movies_json_generated";
insert_genre_movies_generated = "insert into movies_json_generated (json_column, jsonb_column ) select json_column, json_column from  movies_json"
insert_jsonb_only_table = "insert into movies_jsonb select * from movies_json"

moviecount = 0
castcount = 0
actorcount = 0
directory = r'../meta/'

#truncate tables to start fresh:

cursor.execute(truncate_norm_meta)
cursor.execute(truncate_norm_actors)
cursor.execute(truncate_norm_cast)
cursor.execute(truncate_norm_director)
cursor.execute(truncate_norm_genres_tags)
cursor.execute(truncate_norm_genres)

cursor.execute(reset_sequence1)
cursor.execute(reset_sequence2)
cursor.execute(reset_sequence3)
cursor.execute(reset_sequence4)
cursor.execute(reset_sequence5)
cursor.execute(reset_sequence6)
cursor.execute(reset_sequence7)
cursor.execute(reset_sequence8)

cnx.commit()


for myfile in os.scandir(directory):
  if (myfile.path.endswith(".json")):
    with open(myfile.path, "r") as read_file:
     data = json.load(read_file)
    moviecount +=1
    if (moviecount%1000 == 0) :
     current_time = time.perf_counter() - start_time
     print("Processed " + str(moviecount) + " movies so far. " + str(actorcount) + " actors processed. " + str(castcount) + " characters added. Running Time: " + str(current_time)  )
     cnx.commit()
     
    # note you have to send the json as a parameter so chars are escaped, and it must be a tuple otherwise it borks at you

    cursor.execute(insert_json,(data['imdb_id'], json.dumps(data)))

    movie_ai = cursor.fetchone()[0];
    #print('movies lookup: ' + str(movie_ai))
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
            actor_lookup[val] = cursor.fetchone()[0];
            #print('actor lookup: ' + str(actor_lookup[val]))
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
           new_id = cursor.fetchone()[0];
           genre_lookup[val] = new_id
           #print(str(genre_lookup))
       cursor.execute(insert_genre_movies, (genre_lookup[val],movie_ai))
        
            
   
    # commit is needed as default for the connector is autocommit off
    cnx.commit()
    
cursor.execute(truncate_genre_movies_generated)
cursor.execute(insert_genre_movies_generated)
cursor.execute(insert_jsonb_only_table)

cnx.commit()

    
cursor.close()
cnx.close()
