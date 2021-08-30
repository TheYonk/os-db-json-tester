from my_pg_connection import pg_get_read_only_cursor,benchmark_query_simple,benchmark_query_parms_list,benchmark_query_simple_UID,pg_get_cursor,pg_commit,benchmark_query_parms_list_work_around

import time 
import random


cursor = pg_get_read_only_cursor()
cursor2 = pg_get_cursor()

#type A, one big ass table, with minimal columns or indexes
#type B, Generated columns, and expressions 
#type C, Normalized, broken out design

# some indexes are purposfully missing for testing
create_idx1 = "create index idx1  on movies_normalized_meta (title)"
drop_idx1 = " drop index idx1"

create_idx2 = "create index idx2 on movies_json_generated(imdb_rating)"
drop_idx2 = " drop index idx2 "

create_idx3 = "create index idx3 on movies_json_generated (((jsonb_column ->> 'imdb_rating')::numeric))"	
drop_idx3 = " drop index idx3 "

create_idx4 = "CREATE INDEX idx4 ON movies_jsonb USING gin (jsonb_column)"
drop_idx4 = "drop index idx4 "

QRY_TYPE_A_SINGLE_MOVIE =     "select * from movies_jsonb  where jsonb_column @> '{ \"title\" : \"Avengers: Endgame (2019)\"}'"
QRY_TYPE_B_SINGLE_MOVIE =     "select * from movies_json_generated where title = 'Avengers: Endgame (2019)'"
QRY_TYPE_B_EXP_SINGLE_MOVIE = "select * from movies_json_generated where json_column->>'title' = 'Avengers: Endgame (2019)'"
QRY_TYPE_C_SINGLE_MOVIE =     "select * from movies_normalized_meta where title = 'Avengers: Endgame (2019)'"

QRY_TYPE_A_SINGLE_MOVIE_RND10 =     "select * from movies_jsonb  where jsonb_column @> '{ \"title\" : %s }'"
QRY_TYPE_B_SINGLE_MOVIE_RND10 =     "select * from movies_json_generated where title = %s"
QRY_TYPE_B_EXP_SINGLE_MOVIE_RND10 = "select * from movies_json_generated where json_column->>'title' = %s"
QRY_TYPE_C_SINGLE_MOVIE_RND10 =     "select * from movies_normalized_meta where title = %s"

QRY_TYPE_A_TOP_10=     "select * from movies_jsonb order by (jsonb_column->>'imdb_rating')::numeric desc limit 10"
QRY_TYPE_B_TOP_10 =     "select * from movies_json_generated order by imdb_rating desc limit 10"
QRY_TYPE_B_EXP_TOP_10 = "select * from movies_json_generated w  order by (jsonb_column->>'imdb_rating')::numeric desc limit 10"
QRY_TYPE_C_TOP_10 =     "select * from movies_normalized_meta order by imdb_rating desc limit 10"

QRY_TYPE_A_MOVIES_FOR_ACTOR =  "select imdb_id, jsonb_column->>'title', jsonb_column->>'imdb_rating', t.* from movies_jsonb, jsonb_to_recordset((jsonb_column->>'cast'::text)::jsonb) as t(id text,name varchar(100),character text) where  jsonb_column @@ '$.cast.name == \"Robert Downey Jr.\"' and t.name = 'Robert Downey Jr.'"
QRY_TYPE_B_MOVIES_FOR_ACTOR =  "select imdb_id, title, imdb_rating, t.* from movies_json_generated, jsonb_to_recordset((jsonb_column->>'cast'::text)::jsonb) as t(id text,name varchar(100),character text) where name = 'Robert Downey Jr.'"
QRY_TYPE_C_MOVIES_FOR_ACTOR =  "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= 'Robert Downey Jr.'"

QRY_TYPE_A_MOVIES_ACTOR_COUNT = "select t.name, count(*), avg((jsonb_column->>'imdb_rating')::numeric) from movies_jsonb, jsonb_to_recordset((jsonb_column->>'cast'::text)::jsonb) as t(id text,name varchar(100),character text) where t.name = %s group by  t.name "
QRY_TYPE_B_MOVIES_ACTOR_COUNT = "select t.name, count(*), avg(imdb_rating) from movies_json_generated, jsonb_to_recordset((jsonb_column->>'cast'::text)::jsonb) as t(id text,name varchar(100),character text) where t.name = %s group by  t.name "
QRY_TYPE_C_MOVIES_ACTOR_COUNT = "select actor_name, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name = %s group by actor_name "

GET_TITLES_LIST = "select jsonb_column->>'title' from movies_jsonb limit 5000"
GET_ACTORS_LIST = "select actor_name from movies_normalized_actors limit 5000"

loop_amount = 100
current_loop = 0
list_titles = []
list_actors = []
parm_list = []
actor_count_list = [(('Robert Downey Jr.',),),  (('Marlon Brando',),),  (('Tom Cruise', ),), (('Ryan Reynolds',),),  (('Samuel L. Jackson',),)]


#Run Benchmark to compare the queries from both JSON and JSONB using a fixed value
results1 = benchmark_query_simple(QRY_TYPE_A_SINGLE_MOVIE,loop_amount,"Test of simple title select with JSONB Datatype, no GIN",1)

print("creating gin index...")
cursor2.execute(create_idx4)
pg_commit()

results1 = benchmark_query_simple(QRY_TYPE_A_SINGLE_MOVIE,loop_amount,"Test of simple title select with  JSONB Datatype, W/GIN",1)
cursor2.execute(drop_idx4)
pg_commit()


results2 = benchmark_query_simple(QRY_TYPE_B_SINGLE_MOVIE,loop_amount,"Test of simple title select with  Generated Coluimn w/idx",1)
results2 = benchmark_query_simple(QRY_TYPE_B_EXP_SINGLE_MOVIE,loop_amount,"Test of simple title select with  expression idx",1)

results1 = benchmark_query_simple(QRY_TYPE_C_SINGLE_MOVIE,loop_amount,"Test of simple title select normalized table wo idx",1)

print("creating title index...")
cursor2.execute(create_idx1)
pg_commit()
results1 = benchmark_query_simple(QRY_TYPE_C_SINGLE_MOVIE,loop_amount,"Test of simple title select normalized table with idx",1)
cursor2.execute(drop_idx1)
pg_commit()

#Run Benchmark to compare the queries from both JSON and JSONB using a a list of a random 5 titles:

cursor.execute(GET_TITLES_LIST)

for tile_name in cursor:
    list_titles.append(tile_name);

random.seed(time.time())

parm_list.append((random.choice(list_titles),))
parm_list.append((random.choice(list_titles),))
parm_list.append((random.choice(list_titles),))
parm_list.append((random.choice(list_titles),))
parm_list.append((random.choice(list_titles),))
parm_list.append((random.choice(list_titles),))
parm_list.append((random.choice(list_titles),))
parm_list.append((random.choice(list_titles),))
parm_list.append((random.choice(list_titles),))
parm_list.append((random.choice(list_titles),))




#Run Benchmark to compare the queries from both JSON and JSONB using a fixed value
results1 = benchmark_query_parms_list_work_around(QRY_TYPE_A_SINGLE_MOVIE_RND10,loop_amount,"Test of random title select with JSONB Datatype, no GIN",1,parm_list)

print("creating gin index...")
cursor2.execute(create_idx4)
pg_commit()

results1 = benchmark_query_parms_list_work_around(QRY_TYPE_A_SINGLE_MOVIE_RND10,loop_amount,"Test of random title select with  JSONB Datatype, W/GIN",1,parm_list)
cursor2.execute(drop_idx4)
pg_commit()


results2 = benchmark_query_parms_list(QRY_TYPE_B_SINGLE_MOVIE_RND10,loop_amount,"Test of random title select with  Generated Coluimn w/idx",1,parm_list)
results2 = benchmark_query_parms_list(QRY_TYPE_B_EXP_SINGLE_MOVIE_RND10,loop_amount,"Test of random title select with  expression idx",1,parm_list)

results1 = benchmark_query_parms_list(QRY_TYPE_C_SINGLE_MOVIE_RND10,loop_amount,"Test of random title select normalized table wo idx",1,parm_list)

print("creating title index...")
cursor2.execute(create_idx1)
pg_commit()
results1 = benchmark_query_parms_list(QRY_TYPE_C_SINGLE_MOVIE_RND10,loop_amount,"Test of random title select normalized table with idx",1,parm_list)
cursor2.execute(drop_idx1)
pg_commit()

#Run Benchmark to compare the queries from both JSON and JSONB using a fixed value
results1 = benchmark_query_simple(QRY_TYPE_A_TOP_10,loop_amount,"Test of top 10 movies list Single Json, no GIN",1)

print("creating gin index...")
cursor2.execute(create_idx4)
pg_commit()

results1 = benchmark_query_simple(QRY_TYPE_A_TOP_10,loop_amount,"TTest of top 10 movies list Single Json, W/GIN",1)
cursor2.execute(drop_idx4)
pg_commit()

cursor2.execute(create_idx2)
cursor2.execute(create_idx3)

pg_commit()
results2 = benchmark_query_simple(QRY_TYPE_B_TOP_10,loop_amount,"Test of top 10 movies list w  Generated Coluimn w/idx",1)
results2 = benchmark_query_simple(QRY_TYPE_B_EXP_TOP_10,loop_amount,"Test of top 10 movies list w expression idx",1)
cursor2.execute(drop_idx2)
cursor2.execute(drop_idx3)

pg_commit()

results1 = benchmark_query_simple(QRY_TYPE_C_TOP_10,loop_amount,"Test of random title select normalized table w idx",1)


#Run Benchmark to compare the queries from both JSON and JSONB using a fixed value
results1 = benchmark_query_simple(QRY_TYPE_A_MOVIES_FOR_ACTOR,loop_amount,"Test of top 10 movies list Single Json, no GIN",1)

print("creating gin index...")
cursor2.execute(create_idx4)
pg_commit()

results1 = benchmark_query_simple(QRY_TYPE_A_MOVIES_FOR_ACTOR,loop_amount,"TTest of top 10 movies list Single Json, W/GIN",1)
cursor2.execute(drop_idx4)
pg_commit()


results2 = benchmark_query_simple(QRY_TYPE_B_MOVIES_FOR_ACTOR,loop_amount,"Test of top 10 movies list w  Generated Coluimn w/idx",1)
results2 = benchmark_query_simple(QRY_TYPE_B_MOVIES_FOR_ACTOR,loop_amount,"Test of top 10 movies list w expression idx",1)


results1 = benchmark_query_simple(QRY_TYPE_C_MOVIES_FOR_ACTOR,loop_amount,"Test of random title select normalized table w idx",1)

#Run Benchmark to compare the queries from both JSON and JSONB using a fixed value

actor_list = ('Robert Downey Jr.', 'Marlon Brando', 'Tom Cruise', 'Ryan Reynolds', 'Samuel L. Jackson')

results1 = benchmark_query_parms_list(QRY_TYPE_A_MOVIES_ACTOR_COUNT,loop_amount,"Test of actor count JSONB Datatype, no GIN",1,actor_count_list)

print("creating gin index...")
cursor2.execute(create_idx4)
pg_commit()

results1 = benchmark_query_parms_list(QRY_TYPE_A_MOVIES_ACTOR_COUNT,loop_amount,"Test of actor count  JSONB Datatype, W/GIN",1,actor_count_list)
cursor2.execute(drop_idx4)
pg_commit()


results2 = benchmark_query_parms_list(QRY_TYPE_B_MOVIES_ACTOR_COUNT,loop_amount,"Test of actor count Generated Coluimn w/idx",1,actor_count_list)

results1 = benchmark_query_parms_list(QRY_TYPE_C_MOVIES_ACTOR_COUNT,loop_amount,"Test of actor count normalized table wo idx",1,actor_count_list)




#results3 = benchmark_query_parms_list(JSON_QRY_LIST,3,"Test of JSON Datatype",1,parm_list)
#results4 = benchmark_query_parms_list(JSONB_QRY_LIST,3,"Test of JSONB Datatype",1,parm_list)		

#results5 = benchmark_query_simple_UID(UPDATE_JSON_QRY,5,"Test of JSON Datatype",1)
#results6 = benchmark_query_simple_UID(UPDATE_JSONB_QRY,5,"Test of JSONB Datatype",1)	