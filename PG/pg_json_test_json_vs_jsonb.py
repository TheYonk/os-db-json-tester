from my_pg_connection import pg_get_read_only_cursor,benchmark_query_simple,benchmark_query_parms_list,benchmark_query_simple_UID

import time 
import random


cursor = pg_get_read_only_cursor()

JSON_QRY = "select * from movies_json where json_column->>'title' = 'Avengers: Endgame (2019)'"
JSONB_QRY = "select * from movies_jsonb where jsonb_column->>'title' = 'Avengers: Endgame (2019)'"

UPDATE_JSON_QRY = "update movies_json set json_column= json_set(json_column, '{imdb_rating}', '9') where json_column->>'title' = 'Avengers: Endgame (2019)'"
UPDATE_JSONB_QRY = "update movies_jsonb set jsonb_column= jsonb_set(jsonb_column, '{imdb_rating}', '9') where jsonb_column->>'title' = 'Avengers: Endgame (2019)'"

INSERT_JSON_QRY =  "select * from movies_jsonb where jsonb_column->>'title' = 'Avengers: Endgame (2019)'"
INSERT_JSONB_QRY =  "select * from movies_jsonb where jsonb_column->>'title' = 'Avengers: Endgame (2019)'"

JSON_QRY_LIST = "select * from movies_json where json_column->>'title' = %s"
JSONB_QRY_LIST = "select * from movies_jsonb where jsonb_column->>'title' = %s "

GET_TITLES_LIST = "select jsonb_column->>'title' from movies_jsonb limit 5000"

loop_amount = 10
current_loop = 0;
list_titles = [];
parm_list = [];
#Run Benchmark to compare the queries from both JSON and JSONB using a fixed value
results1 = benchmark_query_simple(JSON_QRY,5,"Test of JSON Datatype",1)
results2 = benchmark_query_simple(JSONB_QRY,5,"Test of JSONB Datatype",1)


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

results3 = benchmark_query_parms_list(JSON_QRY_LIST,3,"Test of JSON Datatype",1,parm_list)
results4 = benchmark_query_parms_list(JSONB_QRY_LIST,3,"Test of JSONB Datatype",1,parm_list)		

results5 = benchmark_query_simple_UID(UPDATE_JSON_QRY,5,"Test of JSON Datatype",1)
results6 = benchmark_query_simple_UID(UPDATE_JSONB_QRY,5,"Test of JSONB Datatype",1)	