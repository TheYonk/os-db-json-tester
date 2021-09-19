from my_pg_connection import pg_return_dsn

import time 
import random
import argparse
import threading
import logging
import psycopg2
import string

parser = argparse.ArgumentParser()
parser.add_argument('-t', '--time', dest='time_to_run', type=int, default=300, help='how long should we run this test')
parser.add_argument('-u', '--users', dest='user_count', type=int, default=2, help='how many users do we spin up at once')
parser.add_argument('-s', '--sleep', dest='sleep_timer', type=float, default=0.0, help='how long do we bake in certain dealys actions')
parser.add_argument('-n', '--new', dest='create_new_connection', action="store_true", help='create a new connection every user sim (open & close)')
parser.add_argument('-nq', '--new-con-per-qry', dest='create_new_connection_per_qry', action="store_true", default=0, help='create a new connection every query (open & close)')


args = parser.parse_args()
print(args)

load_ids = "select ai_myid, imdb_id, year from movies_normalized_meta"
load_actors = "select actor_name from movies_normalized_actors where actor_name != ''"
load_titles = "select title, imdb_id, year from movies_normalized_meta"
load_directors = "select director from movies_normalized_director"
comments_cleanup = "truncate movies_normalized_user_comments"


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

MYDSN = pg_return_dsn()

cnx = psycopg2.connect(MYDSN)
#cnx.set_session(readonly=True, autocommit=True)
cursor = cnx.cursor()


cursor.execute(load_ids)
for (ai_myid, imdb_id, mv_year) in cursor:
        ai_myids.append(ai_myid);
        list_ids.append(imdb_id);
        movie_years[imdb_id] = mv_year
        
print("Loading Actors...")

cursor.execute(comments_cleanup)
cnx.commit()
cursor.execute(load_actors)

for actor_name in cursor:
        list_actors.append(actor_name);

print("Loading Titles & ID's...")

cursor.execute(load_titles)
for (movie_titles, imdb_id, mv_year) in cursor:
        list_tiles.append(movie_titles);
        list_ids.append(imdb_id);
        movie_years[imdb_id] = mv_year

def query_db(cnx,sql,parms,fetch):
      x=[]
      cursor = cnx.cursor()
      cursor.execute(sql, parms)      
      if fetch:
         x = cursor.fetchall()
      cnx.commit();
      return(x)

def query_db_new_connect(MYDSN,sql,parms,fetch):
      x=[]
      connect = psycopg2.connect(MYDSN)  
      cursor = connect.cursor()
      cursor.execute(sql, parms)
      if fetch:
         x = cursor.fetchall()
      connect.commit()
      connect.close()
      return(x)

def single_user_actions_v2(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids):
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    
    find_movies_by_actor = "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= %s and actor_name != ''"
    find_movies_by_title = "select imdb_id, title, imdb_rating from movies_normalized_meta a where title = %s"
    find_movies_by_fuzzy_actor = "select imdb_id, title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name like %s"
    simulate_finding_a_movie_record = "select ai_myid, imdb_id, year, title, json_column from movies_normalized_meta where imdb_id = %s"
    simulate_comment_on_movie = "insert into movies_normalized_user_comments (ai_myid, rating, comment ) values ( %s, %s, %s )"
    simulate_updating_movie_record_with_vote = "update movies_normalized_meta set upvotes=upvotes+%s, downvotes=downvotes+%s where ai_myid = %s"    
    
    if create_new_connection : 
        my_query = query_db
        parm1 = psycopg2.connect(MYDSN)  
        
    else :  
        if create_new_connection_per_qry :
           my_query = query_db_new_connect
           parm1 = MYDSN
        else : 
           my_query = query_db
           parm1 = psycopg2.connect(MYDSN)
           parm1.commit()  
           
    letters = string.ascii_lowercase
      
    start_time = time.perf_counter()
 
    while current_time < time_to_run :         
         current_time = time.perf_counter() - start_time    
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = psycopg2.connect(MYDSN)
                
         search_actor = random.choice(list_actors)
         search_title = random.choice(list_tiles)
         search_id = random.choice(list_ids)
         
         x = func_find_movie_by_actor(my_query,parm1,random.choice(list_actors))
         x = func_find_movie_by_actor(my_query,parm1,random.choice(list_actors))
     
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))

         vote = func_generate_vote()
         #print(str(vote))
         comment = func_generate_comment(letters,20,50)
         
         y = func_comment_on_movie(my_query,parm1,x[0][0],vote['uservote'], comment)
         y = func_update_vote_movie(my_query,parm1,x[0][0], vote['upvote'], vote['downvote'])
         
         time.sleep(sleep_timer)
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_up_down_votes(my_query,parm1,(random.choice(list_ids),))
         
         vote = func_generate_vote()
            
         x = func_update_vote_movie(my_query,parm1,x[0][0], vote['upvote'],vote['downvote'])
    if not create_new_connection_per_qry :
        parm1.commit()
    print("Ending Loop....")
    print("Started at..." + str( start_time))
    print("Ended at..." + str( time.perf_counter()))
         
def func_find_movie_by_actor (qry_func,parm1,actor_name) :
        find_movies_by_actor = "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= %s and actor_name != ''"
        x = qry_func(parm1, find_movies_by_actor,actor_name,1)
        return x
        
def func_find_movies_by_title (qry_func,parm1,title_name) :
        find_movies_by_title = "select ai_myid, imdb_id, title, imdb_rating from movies_normalized_meta a where title = %s"
        x = qry_func(parm1, find_movies_by_title,title_name,1)
        return x

def func_comment_on_movie (qry_func,parm1,movieid,vote,comment) :
        simulate_comment_on_movie = "insert into movies_normalized_user_comments (ai_myid, rating, comment ) values ( %s, %s, %s )" 
        myparm =  (movieid,vote,comment)
        qry_func(parm1, simulate_comment_on_movie,myparm,0)
        
def func_find_movies_by_id (qry_func,parm1,id) :
        simulate_finding_a_movie_record = "select ai_myid, imdb_id, year, title, json_column from movies_normalized_meta where imdb_id = %s"
        x = qry_func(parm1, simulate_finding_a_movie_record,id,1)
        return x       

def func_update_vote_movie (qry_func,parm1,id, upvote, downvote) :
        simulate_updating_movie_record_with_vote = "update movies_normalized_meta set upvotes=upvotes+%s, downvotes=downvotes+%s where ai_myid = %s"
        myparm =  (upvote,downvote,id)
        x = qry_func(parm1, simulate_updating_movie_record_with_vote,myparm,0)
        return x     

def func_find_up_down_votes (qry_func,parm1,id ) :
        simulate_updating_movie_record_with_vote = "select ai_myid, imdb_id, title, upvotes, downvotes from  movies_normalized_meta where imdb_id = %s"
        x = qry_func(parm1, simulate_updating_movie_record_with_vote,id,1)
        return x    
        
def func_generate_comment(letters, minsize,maxsize) :
      return "".join(random.choice(letters) for i in range (random.randrange(minsize,maxsize)))

def func_generate_vote() :
      uservote = random.randrange(0,10)
      if uservote < 6 :
         downvote = 1
         upvote = 0
      else :
         downvote = 0
         upvote = 1
      return {"uservote" : uservote, "downvote" : downvote, "upvote" : upvote }
                      
def single_user_actions(MYDSN,time_to_run,sleep_timer,create_new_connection,list_actors,list_tiles,list_ids):
    current_time =0;
    start_time =0;
    qry = 0;
    debug=0
    
    find_movies_by_actor = "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= %s and actor_name != ''"
    find_movies_by_title = "select imdb_id, title, imdb_rating from movies_normalized_meta a where title = %s"
    find_movies_by_fuzzy_actor = "select imdb_id, title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name like %s"
    simulate_finding_a_movie_record = "select ai_myid, imdb_id, year, title, json_column from movies_normalized_meta where imdb_id = %s"
    simulate_comment_on_movie = "insert into movies_normalized_user_comments (ai_myid, rating, comment ) values ( %s, %s, %s )"
    simulate_updating_movie_record_with_vote = "update movies_normalized_meta set upvotes=upvotes+%s, downvotes=downvotes+%s where ai_myid = %s"
    
    if create_new_connection : 
        connect = psycopg2.connect(MYDSN)  
    else :  
        connect = psycopg2.connect(MYDSN) 
        cursor = connect.cursor()
        
    letters = string.ascii_lowercase
         
    start_time = time.perf_counter()
    
    while current_time < time_to_run : 
       current_time = time.perf_counter() - start_time    
       
       if create_new_connection : 
           connect.close()
           connect = psycopg2.connect(MYDSN)
           cursor = connect.cursor()
           
           
       search_actor = random.choice(list_actors)
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
             
       cursor.execute(find_movies_by_fuzzy_actor, (search_actor[0][0:6]+'%',))      
         
       time.sleep(args.sleep_timer)
       
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
       
       time.sleep(args.sleep_timer)
       
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
       connect.commit()    
       
cnt = 0 
threadcount = 0
threadlist = []

while cnt <  args.user_count:   
    threadlist.append(threading.Thread(target=single_user_actions_v2, args=(MYDSN,args.time_to_run,args.sleep_timer,args.create_new_connection,args.create_new_connection_per_qry,list_actors,list_tiles,list_ids)))
    threadcount = threadcount + 1    
    threadlist[-1].start()
    cnt = cnt + 1
    time.sleep(args.sleep_timer)


    
    
#if len(sys.argv) == 1:
#     time_to_run = 300
#else :
#     print ("Run Time Set to: " + sys.argv[1])
#     time_to_run = int(sys.argv[1])
#current_time = 0
#debug = 0
#qry = 0;


