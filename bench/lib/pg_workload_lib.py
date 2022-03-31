import time 
import random
import argparse
import threading
import logging
import psycopg2
import string
import os
import sys
import json

def load_db(MYDSN): 
    print ('inside load function')
    
    load_ids = "select ai_myid, imdb_id, year from movies_normalized_meta"
    load_actors = "select actor_name from movies_normalized_actors where actor_name != ''"
    load_titles = "select title, imdb_id, year from movies_normalized_meta"
    load_directors = "select director from movies_normalized_director"
    comments_cleanup = "truncate movies_normalized_user_comments"
    directors_with_50_movies = "select director from movies_normalized_director group by director having count(*) > 49"
    actors_with_50_movies = "select ai_actor_id from movies_normalized_cast group by ai_actor_id having count(*) > 49"
    actors_with_200_movies = "select ai_actor_id from movies_normalized_cast group by ai_actor_id having count(*) > 199"
    
    returnval = dict()
    # actors list stored here for random searches
    list_actors=[]
    #titles list here for random searches
    list_titles=[]
    #list of ids
    list_ids=[]
    #list of ids
    list_directors=[]
    movie_years= dict()
    ai_myids=[]

    #MYDSN = pg_return_dsn()

    cnx = psycopg2.connect(MYDSN)
    #cnx.set_session(readonly=True, autocommit=True)
    cursor = cnx.cursor()


    cursor.execute(load_ids)
    for (ai_myid, imdb_id, mv_year) in cursor:
            ai_myids.append(ai_myid);
            list_ids.append(imdb_id);
            movie_years[imdb_id] = mv_year
        
    logging.info("Loading Actors...")

    #cursor.execute(comments_cleanup)
    #cnx.commit()
    #func_fill_random_comments(MYDSN,ai_myids,100000)
    
    cursor.execute(load_actors)

    
    for actor_name in cursor:
            list_actors.append(actor_name);

    logging.info("Loading Titles & ID's...")

    cursor.execute(load_titles)
    for (movie_titles, imdb_id, mv_year) in cursor:
            list_titles.append(movie_titles);
            list_ids.append(imdb_id);
            movie_years[imdb_id] = mv_year
    
    logging.info("Loading Movies and Directors with Many Movies")
    
    cursor.execute(directors_with_50_movies)
    list_50_directors = cursor.fetchall()
    cursor.execute(actors_with_50_movies)
    list_50_actors = cursor.fetchall()
    cursor.execute(actors_with_200_movies)
    list_200_actors = cursor.fetchall()
 
    
    returnval['list_actors'] = list_actors
    returnval['list_titles'] = list_titles
    returnval['list_ids'] = list_ids
    returnval['ai_myids'] = ai_myids
    returnval['directors_with_50_movies'] = list_50_directors
    returnval['actors_with_50_movies'] = list_50_actors
    returnval['actors_with_200_movies'] = list_200_actors
    
    return returnval

def query_db(cnx,sql,parms,fetch):
    x=[]
    try:
      cursor = cnx.cursor()
      cursor.execute(sql, parms)      
      if fetch:
         x = cursor.fetchall()
      cnx.commit();
    except:
        logging.error("Issue was raised in the query db function")  
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

def new_connection(MYDSN):
    connect = psycopg2.connect(MYDSN)  
    return connect
      
      
def single_user_actions_v2(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,default_values,activelist1,mytime,mycount,wid):
   try:    
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    pid = os.getpid()
    logging.info("pid: %s" , pid)
    logging.info("Active List: %s", str(activelist1))
    
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
    count = 0
 
    while activelist1[os.getpid()] == 1 :   
         current_time = time.perf_counter()
         count = count +1; 
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

         myid = x[0][0]
         
         x = func_find_movie_comments (my_query,parm1,myid)
         
         vote = func_generate_vote()
         #logging.info(str(vote))
         comment = func_generate_comment(letters,20,50)
         
         y = func_comment_on_movie(my_query,parm1,myid,vote['uservote'], comment)
         y = func_update_vote_movie(my_query,parm1,myid, vote['upvote'], vote['downvote'])
         
         time.sleep(sleep_timer)
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_movie_comments (my_query,parm1,x[0][0])
         
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_up_down_votes(my_query,parm1,(random.choice(list_ids),))
         
         vote = func_generate_vote()
            
         x = func_update_vote_movie(my_query,parm1,x[0][0], vote['upvote'],vote['downvote'])
         mycount[wid] = mycount[wid] + 1
         mytime[wid] =  round(mytime[wid] +(time.perf_counter() -current_time),4)
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
    del activelist1[os.getpid()]
    exit()
   except:
     logging.error("This web thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
      
def single_user_actions_solo(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids):
   try:
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    runme = 1
    
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
    count = 0
    while runme == 1 :         
         current_time = time.perf_counter()
         count = count +1;
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

         myid = x[0][0]
         
         x = func_find_movie_comments (my_query,parm1,myid)
         
         vote = func_generate_vote()
         #logging.info(str(vote))
         comment = func_generate_comment(letters,20,50)
         
         y = func_comment_on_movie(my_query,parm1,myid,vote['uservote'], comment)
         y = func_update_vote_movie(my_query,parm1,myid, vote['upvote'], vote['downvote'])
         
         time.sleep(sleep_timer)
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_movie_comments (my_query,parm1,x[0][0])
         
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_up_down_votes(my_query,parm1,(random.choice(list_ids),))
         
         vote = func_generate_vote()
            
         x = func_update_vote_movie(my_query,parm1,x[0][0], vote['upvote'],vote['downvote'])
         runme = 2
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
   except:
     logging.error("This web thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )          

def report_user_actions(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,default_values,activelist2,mytime,mycount,wid):
   try:    
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    
    logging.info("pid: %s" , os.getpid())
    logging.info("Active List: %s" , str(activelist2))
   
    
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
    count = 0
    while activelist2[os.getpid()] == 1 :         
         current_time = time.perf_counter()
         count = count +1;
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = psycopg2.connect(MYDSN)
                
         search_actor = random.choice(list_actors)
         search_title = random.choice(list_tiles)
         search_id = random.choice(list_ids)
         year1 = random.randrange(1900, 2015, 5)
         year2 =  year1 + 5	
         
         
         x = func_rpt_ratings_over_year_range(my_query,parm1,year1,year2)
         x = func_rpt_movies_by_year(my_query,parm1)
         x = func_rpt_directors_year_range(my_query,parm1,year1,year2)
         x = func_rpt_movies_for_actor(my_query,parm1,random.choice(list_actors))
         x = func_rpt_movies_for_actor_fuzzy(my_query,parm1,random.choice(list_actors))
         x = func_rpt_movies_for_actor_year(my_query,parm1,random.choice(list_actors),year1,year2)
         x = func_rpt_movies_per_country_year(my_query,parm1,year1,year2)
         x = func_rpt_material_vote_count(my_query,parm1,search_title)
         mycount[wid] = mycount[wid] + 1
         mytime[wid] =  round(mytime[wid] +(time.perf_counter() -current_time),4)
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
    del activelist2[os.getpid()]
    exit()
   except:
     logging.error("This reporting thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )    

def insert_update_delete(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,default_values,activelist3,mytime,mycount,wid):
   try: 
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    logging.info("pid: %s" , os.getpid())
    logging.info("Active List: %s" , str(activelist3))
   
    
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
    count = 0  
    start_time = time.perf_counter()
 
    while activelist3[os.getpid()] == 1 :         
         current_time = time.perf_counter()
         count = count +1; 
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = psycopg2.connect(MYDSN)
                
         search_actor = random.choice(list_actors)
         search_title = random.choice(list_tiles)
         search_id = random.choice(ai_myids)
         
         x = func_fill_random_comments(MYDSN,ai_myids,10) 
         x = func_update_random_comments(MYDSN, ai_myids, 5) 
         x = func_delete_random_comments(MYDSN,ai_myids,6) 
         time.sleep(sleep_timer)
         
         
         r = random.randint(1,3)
         if r == 1:
            x = func_fill_random_comments(MYDSN,ai_myids,5)
         if r == 2:
            x = func_delete_random_comments(MYDSN,ai_myids,2)
         if r == 3 :
            x = func_update_random_comments(MYDSN, ai_myids, 2) 
         mycount[wid] = mycount[wid] + 1
         mytime[wid] =  round(mytime[wid] +(time.perf_counter() -current_time),4)
         time.sleep(sleep_timer)
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
    del activelist3[os.getpid()]
    exit()
   except:
     logging.error("This chat/comments thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  ) 

def long_transactions(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,default_values,activelist4,mytime,mycount,wid):
   try: 
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    logging.info("pid: %s" , os.getpid())
    logging.info("Active List: %s" , str(activelist4))
   
    
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
    count = 0 
    start_time = time.perf_counter()
 
    while activelist4[os.getpid()] == 1 :         
         current_time = time.perf_counter()
         count = count +1; 
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = psycopg2.connect(MYDSN)
                
         search_actor = random.choice(list_actors)
         search_title = random.choice(list_tiles)
         search_id = random.choice(list_ids)
         year1 = random.randrange(1900, 2015, 5)
         year2 =  year1 + 10	
         locktime = (random.randint(50,1000)/100) 
         
         r = random.randint(1,4)
         if r == 1:
            x = func_really_bad_for_update (MYDSN, locktime, year1, year2)
         if r == 2:
            x = func_find_movie_by_actor_for_update (MYDSN,search_actor,locktime)
         if r == 3 :
            x = func_find_update_meta (MYDSN, locktime, search_title) 
         if r == 4 :
            x = func_find_update_comments (MYDSN,locktime)
         mycount[wid] = mycount[wid] + 1
         mytime[wid] =  round(mytime[wid] +(time.perf_counter() -current_time),4)
 
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
    del activelist4[os.getpid()]
    exit()
   except:
     logging.error("This reporting thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  ) 

def single_user_actions_special(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids):
    #testing out stump an expert workload...
   try: 
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    runme = 1
    
    find_movies_by_actor = "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= %s and actor_name != ''"
    find_movies_by_title = "select imdb_id, title, imdb_rating from movies_normalized_meta a where title = %s"
    find_movies_by_fuzzy_actor = "select imdb_id, title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name like %s"
    simulate_finding_a_movie_record = "select ai_myid, imdb_id, year, title, json_column from movies_normalized_meta where imdb_id = %s"
    simulate_comment_on_movie = "insert into movies_normalized_user_comments (ai_myid, rating, comment ) values ( %s, %s, %s )"
    simulate_updating_movie_record_with_vote = "update movies_normalized_meta set upvotes=upvotes+%s, downvotes=downvotes+%s where ai_myid = %s"    
    
    if create_new_connection : 
        my_query = query_db
        parm1 = mysql.connector.connect(**config) 
        
    else :  
        if create_new_connection_per_qry :
           my_query = query_db_new_connect
           parm1 = config
        else : 
           my_query = query_db
           parm1 = mysql.connector.connect(**config)
           parm1.commit()  
           
    letters = string.ascii_lowercase
      
    start_time = time.perf_counter()
 
    while runme == 1 :         
         current_time = time.perf_counter()
         count = count +1; 
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = mysql.connector.connect(**config)
                
         search_actor = random.choice(list_actors)
         search_title = random.choice(list_tiles)
         search_id = random.choice(list_ids)
         
         x = func_find_movie_by_actor(my_query,parm1,random.choice(list_actors))
         x = func_find_movie_by_actor(my_query,parm1,random.choice(list_actors))
         x = func_find_actors_and_characters_by_title(my_query,parm1,(random.choice(list_tiles),))   
     
         x = func_check_comment_rates_last_week(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         myid = x[0][0]
         
         x = func_find_movie_comments (my_query,parm1,myid)
         
         vote = func_generate_vote()
         #logging.info(str(vote))
         comment = func_generate_comment(letters,20,50)
         year1 = random.randrange(1900, 2015, 5)
         year2 =  year1 + 5
         
         
         y = func_comment_on_movie(my_query,parm1,myid,vote['uservote'], comment)
         y = func_update_vote_movie(my_query,parm1,myid, vote['upvote'], vote['downvote'])
         
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_movie_comments (my_query,parm1,x[0][0])
         x = func_check_comment_rates_last_week(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_up_down_votes(my_query,parm1,(random.choice(list_ids),))
         vote = func_generate_vote()
         x = func_update_vote_movie(my_query,parm1,x[0][0], vote['upvote'],vote['downvote'])
         x = func_check_comment_rates_last_week_actor(my_query,parm1,random.choice(list_actors)) 
         x = func_find_movies_dtls_country_year(my_query,parm1,year1,year2)
         x = func_find_actors_and_characters_by_title(my_query,parm1,(random.choice(list_tiles),))
         
         runme = 2
    if not create_new_connection_per_qry :
        parm1.commit()
   except:
     logging.error("This reporting thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )

def read_only_user_actions(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,default_values,myactivelist,mytime,mycount,wid):
   try: 
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    error = 0
    logging.info("pid: %s" , os.getpid())
    logging.info("Active List: %s" , str(myactivelist))
    
    
     
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
    count = 0 
    start_time = time.perf_counter()
 

    while myactivelist[os.getpid()] == 1 :         
       #try:
         current_time = time.perf_counter()
         count = count +1;
         if error == 1 :
             logging.info('Starting Over! ', os.getpid())
             error = 0
         current_time = time.perf_counter()  
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = psycopg2.connect(MYDSN)
                
         search_actor = random.choice(default_values['actors_with_50_movies'])
         #print(search_actor)
         search_director = random.choice(default_values['directors_with_50_movies'])
         #print(search_director)
         
         search_title = random.choice(list_tiles)
         search_id = random.choice(list_ids)
         year1 = random.randrange(1900, 2015, 1)
         year2 = random.randrange(1900, 2015, 1)
         
         x = func_find_movie_by_actor(my_query,parm1,random.choice(list_actors))
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_actors_and_characters_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_top_movies_year(my_query,parm1,(year1,))
         x = func_movies_by_director(my_query,parm1,search_director)
         x = func_movies_by_actor_with_many(my_query,parm1,search_actor)
         
         x = func_find_movie_by_actor(my_query,parm1,random.choice(list_actors))
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_actors_and_characters_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_top_movies_year(my_query,parm1,(year2,))
         x = func_movies_by_director(my_query,parm1,random.choice(default_values['directors_with_50_movies']))
         x = func_movies_by_actor_with_many(my_query,parm1,random.choice(default_values['actors_with_50_movies']))
         mycount[wid] = mycount[wid] + 1
         mytime[wid] =  round(mytime[wid] +(time.perf_counter() -current_time),4)
       #except Exception as e:
       #    logging.error("error: %s", e)
       #    z = sys.exc_info()[0]
       #    logging.error("systems: %s",z )  
       #except: 
       #    logging.info("error in read only workload")
       #    logging.info('pid:', os.getpid())
       #    error = 1
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
    del myactivelist[os.getpid()]
    exit()
   except:
     logging.error("This read only thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  ) 
     
def single_user_actions_special_v2(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,default_values,myactivelist,mytime,mycount,wid):
   try: 
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    error = 0
    logging.info("pid: %s" , os.getpid())
    logging.info("Active List: %s" , str(myactivelist))
   
    
     
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
    count = 0

    while myactivelist[os.getpid()] == 1 :         
       #country:
         current_time = time.perf_counter()
         count = count +1;
         if error == 1 :
             logging.info('Starting Over! ', os.getpid())
             error = 0
         current_time = time.perf_counter()
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = psycopg2.connect(MYDSN)
                
         search_actor = random.choice(list_actors)
         search_title = random.choice(list_tiles)
         search_id = random.choice(list_ids)
         
         x = func_find_movie_by_actor(my_query,parm1,random.choice(list_actors))
         x = func_find_actors_and_characters_by_title(my_query,parm1,(random.choice(list_tiles),))   
         x = func_check_comment_rates_last_week(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         myid = x[0][0]
         x = func_find_movie_comments (my_query,parm1,myid)
         year1 = random.randrange(1900, 2015, 5)
         year2 =  year1 + 5
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_movie_comments (my_query,parm1,x[0][0])
         x = func_check_comment_rates_last_week(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_up_down_votes(my_query,parm1,(random.choice(list_ids),))
         x = func_check_comment_rates_last_week_actor(my_query,parm1,random.choice(list_actors)) 
         x = func_find_movies_dtls_country_year(my_query,parm1,year1,year2)
         x = func_find_actors_and_characters_by_title(my_query,parm1,(random.choice(list_tiles),))
         mycount[wid] = mycount[wid] + 1
         mytime[wid] =  round(mytime[wid] +(time.perf_counter() -current_time),4)
       #except: 
       #    logging.info("error in special workload")
       #    logging.info('pid:', os.getpid())
       #    error = 1
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
    del myactivelist[os.getpid()]
    exit()
   except:
     logging.error("This special thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  ) 
    
def multi_row_returns(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,default_values,myactivelist,mytime,mycount,wid):
   try:    
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    error = 0
    logging.info("MR: pid: %s" , os.getpid())
    logging.info("MR: Active List: %s" , str(myactivelist))
   
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
    count = 0
    while myactivelist[os.getpid()] == 1 : 
       current_time = time.perf_counter()
       count = count +1;            
       try:
         if error == 1 :
             logging.info('Starting Over! ', os.getpid())
             error = 0
         current_time = time.perf_counter() 
         if create_new_connection : 
             parm1.commit()  
             parm1.close()
             my_query = query_db
             parm1 = psycopg2.connect(MYDSN)
                
         search_actor = random.choice(list_actors)
         search_title = random.choice(list_tiles)
         search_id = random.choice(list_ids)
         year1 = random.randrange(1900, 2015, 1)
         year2 = random.randrange(1900, 2015, 1)
         

         x = func_find_actors_and_characters_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_top_movies_year(my_query,parm1,(year1,))
         x = func_movies_by_director(my_query,parm1,random.choice(default_values['directors_with_50_movies']))
         x = func_movies_by_actor_with_many(my_query,parm1,random.choice(default_values['actors_with_50_movies']))
         x = func_movies_by_actor_with_many(my_query,parm1,random.choice(default_values['actors_with_200_movies']))
         mycount[wid] = mycount[wid] + 1
         mytime[wid] =  round(mytime[wid] +(time.perf_counter() -current_time),4)
         
       except: 
            logging.info("error in multi-row only workload")
            logging.info('pid:', os.getpid())
            error = 1
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
    del myactivelist[os.getpid()]
    exit()
   except:
     logging.error("This multi-row thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )     
        
def read_only_user_json_actions(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,default_values,myactivelist,mytime,mycount,wid):
    
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    error = 0
    logging.info("pid: %s" , os.getpid())
    logging.info("Active List: %s" , str(myactivelist))
   
     
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
    count = 0 
    start_time = time.perf_counter()
 

    while myactivelist[os.getpid()] == 1 :         
       try:
         if error == 1 :
             logging.info('Starting Over! ', os.getpid())
             error = 0
         current_time = time.perf_counter()    
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = psycopg2.connect(MYDSN)
                
         search_actor = random.choice(default_values['actors_with_50_movies'])
         #print(search_actor)
         search_director = random.choice(default_values['directors_with_50_movies'])
         #print(search_director)
         
         search_title = random.choice(list_tiles)
         search_id = random.choice(list_ids)
         year1 = random.randrange(1900, 2015, 1)
         year2 = random.randrange(1900, 2015, 1)
         
         x = func_find_movie_by_actor(my_query,parm1,random.choice(list_actors))
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_actors_and_characters_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_top_movies_year(my_query,parm1,(year1,))
         x = func_movies_by_director(my_query,parm1,search_director)
         x = func_movies_by_actor_with_many(my_query,parm1,search_actor)
         
         x = func_find_movie_by_actor(my_query,parm1,random.choice(list_actors))
         x = func_find_movies_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_find_movies_by_id(my_query,parm1,(random.choice(list_ids),))
         x = func_find_actors_and_characters_by_title(my_query,parm1,(random.choice(list_tiles),))
         x = func_top_movies_year(my_query,parm1,(year2,))
         x = func_movies_by_director(my_query,parm1,random.choice(default_values['directors_with_50_movies']))
         x = func_movies_by_actor_with_many(my_query,parm1,random.choice(default_values['actors_with_50_movies']))
         mycount[wid] = mycount[wid] + 1
         mytime[wid] =  round(mytime[wid] +(time.perf_counter() -current_time),4)
         
       except: 
           logging.info("error in read only workload")
           logging.info('pid:', os.getpid())
           error = 1
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
    del myactivelist[os.getpid()]
    exit()
    

def insert_update_logs (MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,default_values,myactivelist,mytime,mycount,wid):
   try:    
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    error = 0
    logging.info("MR: pid: %s" , os.getpid())
    logging.info("MR: Active List: %s" , str(myactivelist))
   
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
    count = 0
    while myactivelist[os.getpid()] == 1 : 
       current_time = time.perf_counter()
       count = count +1;            
       try:
         if error == 1 :
             logging.info('Starting Over! ', os.getpid())
             error = 0
         current_time = time.perf_counter() 
         if create_new_connection : 
             parm1.commit()  
             parm1.close()
             my_query = query_db
             parm1 = psycopg2.connect(MYDSN)
         
         x = func_log_movie_view (my_query,parm1,MYDSN,ai_myids,100)
         x = func_del_log_movie_view (my_query,parm1,MYDSN,list_ids)

         mycount[wid] = mycount[wid] + 1
         mytime[wid] =  round(mytime[wid] +(time.perf_counter() -current_time),4)
         
       except: 
            logging.info("error in loggin workload")
            logging.info('pid:', os.getpid())
            error = 1
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    logging.info("Ending Loop....")
    logging.info("Started at..." + str( start_time))
    logging.info("Ended at..." + str( time.perf_counter()))
    mytime1 = round(time.perf_counter()-start_time,3);
    timeper = round(mytime1/count,2)
    logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )
    del myactivelist[os.getpid()]
    exit()
   except:
     logging.error("This multi-row thread failed.... ooooops")  
     mytime1 = round(time.perf_counter()-start_time,3);
     timeper = round(mytime1/count,2)
     logging.info("Thread:  %s Times looped: %s Time per Loop : %s total time: %s", wid, count, timeper, mytime1  )     


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
        
def func_find_movie_comments (qry_func,parm1,id ) :
        query = "select comment_id, ai_myid, comment, comment_add_time, comment_update_time from movies_normalized_user_comments where ai_myid = %s order by comment_add_time desc"
        x = qry_func(parm1, query,(id,),1)
        return x    



def func_create_materialized_view_votes(mydsn) :
    createmtlv = "create materialized view if not exists  view_voting_counts as select a.ai_myid, title, a.imdb_id, count(*) as comment_count, max(rating) as max_rate, avg(rating) as avg_rate, sum(upvotes) as upvotes, sum(downvotes) as downvotes from movies_normalized_user_comments a join movies_normalized_meta b on a.ai_myid=b.ai_myid::int  group by a.ai_myid, title, a.imdb_id";        
    x = query_db_new_connect(mydsn, createmtlv, (), 0)
    return(x)
    
def func_refreshed_materialized_view_votes(mydsn) :
    createmtlv = "REFRESH MATERIALIZED VIEW view_voting_counts";        
    x = query_db_new_connect(mydsn, createmtlv, (), 0)
    return(x)
    
def func_create_materialized_view_movies_by_year(mydsn)  :
    createmtlv = "create materialized view if not exists view_year_counts as select year, count(distinct b.imdb_id) as movie_count, avg(imdb_rating) as avg_imdb_rating,  max(rating) as max_rate, avg(rating) as avg_rate, sum(upvotes) as upvotes, sum(downvotes) as downvotes from movies_normalized_user_comments a join movies_normalized_meta b on a.ai_myid=b.ai_myid::int  group by year";        
    x = query_db_new_connect(mydsn, createmtlv, (), 0)
    return(x)
    
def func_refreshed_materialized_view_movies_by_year(mydsn)  :            
    createmtlv = "REFRESH MATERIALIZED VIEW view_year_counts";        
    x = query_db_new_connect(mydsn, createmtlv, (), 0)
    return(x)
        
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


def func_rpt_ratings_over_year_range (qry_func,parm1,year1,year2) :
        find_movie_ratings_count_for_years = "select count(*), imdb_rating from movies_normalized_meta where year > %s  and year < %s group by imdb_rating"
        x = qry_func(parm1, find_movie_ratings_count_for_years,(year1,year2),1)
        return x

def func_rpt_movies_by_year (qry_func,parm1) :
        find_movies_by_years = "select count(*), year from movies_normalized_meta group by year order by year desc"
        x = qry_func(parm1, find_movies_by_years,(),1)
        return x

def func_rpt_directors_year_range (qry_func,parm1,year1,year2) :
        find_movies_and_directors_by_years = "select year, director, count(*), avg(imdb_rating) from movies_normalized_director a join movies_normalized_meta b on a.ai_myid = b.ai_myid where  year > %s and year < %s  group by year, director "
        x = qry_func(parm1, find_movies_and_directors_by_years,(year1,year2),1)
        return x
        
def func_rpt_movies_for_actor(qry_func,parm1,actor) :
         actor_movie_count_by_year = "select actor_name, year, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name = %s group by year, actor_name"
         x = qry_func(parm1, actor_movie_count_by_year,actor,1)
         return x                       

def func_rpt_movies_for_actor_fuzzy(qry_func,parm1,actor) :
         actor2 = actor[0][0:6]+'%'
         actor_movie_count_by_year_fuzzy = "select actor_name, year, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name like %s group by year, actor_name"
         x = qry_func(parm1, actor_movie_count_by_year_fuzzy,(actor2,),1)
         return x    
         
def func_rpt_movies_for_actor_year(qry_func,parm1,actor,year1,year2) :
         #logging.info('actor:', str(actor))
         actor_movie_count_by_year_range = "select actor_name, year, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and year between %s and %s group by year, actor_name"
         try: 
             x = qry_func(parm1, actor_movie_count_by_year_range,(year1,year2),1)
         except Exception as e:
             x = []
             logging.error("error: %s" ,e)
             z = sys.exc_info()[0]
             logging.error("systems: %s",z )
         return x           

def func_rpt_movies_per_country_year(qry_func,parm1,year1,year2) :
          find_country_movies_range_year = "select year, country, count(*) from movies_normalized_meta where year between %s and %s  group by year, country"
          x = qry_func(parm1, find_country_movies_range_year,(year1,year2),1)
          return x
        
def func_rpt_material_vote_count(qry_func,parm1,title) :
          title2 = title[0][0:3]+'%'
          get_all_votes_fuzzy = "select * from view_voting_counts where title like %s"
          x = qry_func(parm1, get_all_votes_fuzzy,(title2,),1)
          return x
                              
def func_find_movie_by_actor_for_update (MYDSN,actor_name, sleeptime) :
        myconnect = new_connection(MYDSN)
        mycursor = myconnect.cursor()
        find_movies_by_actor = "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= %s and actor_name != '' for update"
        mycursor.execute(find_movies_by_actor, actor_name)
        x = mycursor.fetchall()
        time.sleep(sleeptime)
        myconnect.commit()
        return x
        
def func_really_bad_for_update (MYDSN, sleeptime, year1, year2) :
        myconnect = new_connection(MYDSN)
        mycursor = myconnect.cursor()
        query = "select ai_myid, title, imdb_rating from movies_normalized_meta where year > %s and year < %s order by imdb_rating desc limit 25 for update  "
        mycursor.execute(query, (year1,year2))
        x = mycursor.fetchall()
        time.sleep(sleeptime)
        myconnect.commit()
        return x        

def func_find_update_meta (MYDSN, sleeptime, title) :
        myconnect = new_connection(MYDSN)
        mycursor = myconnect.cursor()
        title2 = title[0][0:3]+'%'
        query = "select ai_myid,title, imdb_rating, year from movies_normalized_meta where title like %s for update  "
        update = " update movies_normalized_meta set upvotes = upvotes + 1, imdb_rating = imdb_rating * 1, year = year where ai_myid = %s  "
        mycursor.execute(query, (title2,))
        x = mycursor.fetchall()
        time.sleep(sleeptime)
        y = random.choice(x)
        mycursor.execute(update, (y[0],))
        time.sleep(sleeptime)
        myconnect.commit()
        return x   
             
def func_find_update_comments (MYDSN,sleeptime) :
        myconnect = new_connection(MYDSN)
        mycursor = myconnect.cursor()
        query = "select comment_id, ai_myid, comment_add_time, comment_update_time from movies_normalized_user_comments order by comment_add_time desc limit 25 for update"
        update = "update movies_normalized_user_comments set comment = comment, rating = rating * 1, comment_update_time = current_timestamp where comment_id = %s"
        delete = "delete from movies_normalized_user_comments where comment_id = %s"
        mycursor.execute(query, ())
        x = mycursor.fetchall()
        time.sleep(sleeptime)
        if(len(x) >0):
          y = x.pop()
          mycursor.execute(update, (y[0],))
        if(len(x) >0):
          y = x.pop()
          mycursor.execute(update, (y[0],))
        if(len(x) >0):
          y = x.pop()
          mycursor.execute(delete, (y[0],))
        time.sleep(sleeptime/2)
        myconnect.commit()
        return x        

def func_fill_random_comments (MYDSN,list_ids,comments_to_add) :
    
     myconnect = new_connection(MYDSN)
     mycursor = myconnect.cursor()
     letters = string.ascii_lowercase
     i = 0
     while i < comments_to_add :
         i = i + 1
         comment = func_generate_comment(letters,20,50)
         vote = func_generate_vote()
         search_id = random.choice(list_ids)
         simulate_comment_on_movie = "insert into movies_normalized_user_comments (ai_myid, rating, comment ) values ( %s, %s, %s )" 
         mycursor.execute(simulate_comment_on_movie, (search_id,vote['uservote'], comment))
     
     myconnect.commit();             
     return i;

def func_delete_random_comments (MYDSN,list_ids,comments_to_remove) :
    
     myconnect = new_connection(MYDSN)
     mycursor = myconnect.cursor()
     letters = string.ascii_lowercase
     i = 0
     while i < comments_to_remove :
         i = i + 1
         search_id = random.choice(list_ids)
         delete = "delete from movies_normalized_user_comments where comment_id in (select comment_id from movies_normalized_user_comments where ai_myid = %s limit 3  )" 
         #logging.info('delete id: ',search_id)
         mycursor.execute(delete, (search_id,))
     myconnect.commit();             
     return i;


def func_update_random_comments (MYDSN,list_ids,comments_to_update) :
        myconnect = new_connection(MYDSN)
        mycursor = myconnect.cursor()
        i = 0
        while i < comments_to_update :
            i = i + 1
            search_id = random.choice(list_ids)
            update = "update movies_normalized_user_comments set comment = comment, rating = rating * 1, comment_update_time = current_timestamp where ai_myid = %s"
            mycursor.execute(update, (search_id,))
        myconnect.commit();             
        return i;
        
     
def run_vacuum (MYDSN):
     myconnect = new_connection(MYDSN)
     myconnect.set_session(autocommit=True)
     mycursor = myconnect.cursor()
    
    
     query = "vacuum full analyze movies_normalized_meta"
     x = mycursor.execute(query, ())
     
     query = "vacuum full analyze movies_normalized_user_comments"
     x = mycursor.execute(query, ())
     
     return x
     
def make_copy_of_table (MYDSN):
     query = "DROP TABLE IF EXISTS copy_of_json_table"
     x = query_db_new_connect(MYDSN, query, (), 0)
     
     query = "create table copy_of_json_table ( ai_myid serial primary key, imdb_id varchar(255) generated always as (jsonb_column ->> 'imdb_id') stored, title varchar(255) generated always as (jsonb_column ->> 'title') stored,imdb_rating decimal(5,2) generated always as ((jsonb_column  ->> 'imdb_rating')::numeric) stored,	overview text generated always as (jsonb_column ->> 'overview') stored,	director jsonb generated always as ((jsonb_column ->> 'director')::json) stored, country varchar(100) generated always as (jsonb_column ->> 'country') stored, jsonb_column jsonb, json_column json) "
     x = query_db_new_connect(MYDSN, query, (), 0)
     
     query = "insert into copy_of_json_table (jsonb_column, json_column) select jsonb_column, json_column from movies_json_generated"
     x = query_db_new_connect(MYDSN, query, (), 0)
     
     return x

def refresh_all_mat_views (MYDSN):
    
    logging.debug('-- inside Rebuild Mat Views')
    logging.debug('-- Create 1 Rebuild Mat Views')
    x = func_create_materialized_view_votes(MYDSN)
    logging.debug('-- refresh 1 Mat Views')
    x = func_refreshed_materialized_view_votes(MYDSN)
    logging.debug('-- Create 2 Rebuild Mat Views')
    x = func_create_materialized_view_movies_by_year(MYDSN)
    logging.debug('-- refresh 2 Mat Views')
    x = func_refreshed_materialized_view_movies_by_year(MYDSN)
    return x

def func_pk_bigint (MYDSN) :
            x = 0
            
            logging.debug('Dropping Mat View Year_counts')
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_year_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
            
            logging.debug('Dropping Mat View view_voting_counts')
            
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_voting_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
            
            alter_pk = "alter table movies_normalized_meta alter column ai_myid type bigint using ai_myid::bigint"
            logging.info('Changing PK to  bigint') 
            x = query_db_new_connect(MYDSN, alter_pk,(),0)
            
            logging.debug('Rebuild Mat Views, refresh')
            
            refresh_all_mat_views(MYDSN)
            
            logging.info('done Changing PK') 
            
            return x

def func_pk_int (MYDSN) :
            logging.debug('Dropping Mat View view_year_counts')
            
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_year_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
            
            logging.debug('Dropping Mat View view_voting_counts')
            
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_voting_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
                
            alter_pk = "alter table movies_normalized_meta alter column ai_myid type int using ai_myid::int"
            logging.info('Changing PK to  int') 
            x = query_db_new_connect(MYDSN, alter_pk,(),0)
            
            logging.debug('Rebuild Mat Views, refresh')
            
            refresh_all_mat_views(MYDSN)
            logging.info('done Changing PK') 
            
            return x
            
def func_pk_varchar (MYDSN) :
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_year_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
            
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_voting_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
                
            alter_pk = "alter table movies_normalized_meta alter column ai_myid type varchar(32) using ai_myid::varchar "
            logging.info('Changing PK to  Varchar') 
            x = query_db_new_connect(MYDSN, alter_pk,(),0)
            refresh_all_mat_views(MYDSN)
            logging.info('done Changing PK') 
            
            return x

def func_load_voting_count_hisory(config) :
                  load_etl = "insert into voting_count_history select ai_myid, now(), title, imdb_id, comment_count, max_rate, avg_rate, upvotes, downvotes from view_voting_counts"
                  purge_old = "delete from voting_count_history where store_time < current_date - interval '30' day"
                  logging.info('executing load of count history') 
                  try:
                     x = query_db_new_connect(config, load_etl,(),0)
                     logging.info('finishing count history') 
                  
                     x = query_db_new_connect(config, purge_old,(),0)
                     logging.info('finishing purging count history') 
                     return x
                  except psycopg2.Error as e:
                      logging.error("error loading voting_count_history")
                      print(e.pgerror)
                      return -1
                  except:
                     logging.error("error loading voting_count_history")
                     return -1
                  
def func_find_actors_and_characters_by_title (qry_func,parm1,title_name) :
        find_movies_by_title = "select title, imdb_rating, actor_name, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and title =%s"
        x = qry_func(parm1, find_movies_by_title,title_name,1)
        return x       
        
def func_find_movies_dtls_country_year(qry_func,parm1,year1,year2) :
          country_list = [ 'Mexico', 'Canada', 'France', 'USA', 'China', 'Ireland', 'Netherlands', 'UK', 'Germany']
          mycountry = random.choice(country_list)
          find_country_dtls_year = "Select title, imdb_rating from movies_normalized_meta where country = %s and year > %s and year < %s order by imdb_rating desc limit 100"
          x = qry_func(parm1, find_country_dtls_year,(mycountry,year1,year2),1)
          return x     
          
def func_check_comment_rates_last_week(qry_func,parm1,title_name) :
   find_count_changes = "select upvotes, downvotes, comment_count, avg_rate from voting_count_history where title = %s and store_time > current_date - 7 order by store_time desc"
   x = qry_func(parm1, find_count_changes,title_name,1)
   return x
   
def func_check_comment_rates_last_week_actor(qry_func,parm1,actor_name) :
   find_count_changes = "select actor_name, count(distinct title), sum(upvotes), sum(downvotes), sum(comment_count), avg(avg_rate) from voting_count_history a,  movies_normalized_cast b,  movies_normalized_actors c  where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name = %s and store_time > current_date - 7 group by actor_name, store_time order by store_time desc" 
   x = qry_func(parm1, find_count_changes,actor_name,1)
   return x
   
def func_top_movies_year (qry_func,parm1,myyear) :
        find_movies_by_years = "select year, title, imdb_rating from movies_normalized_meta where year = %s order by imdb_rating desc limit 50"
        x = qry_func(parm1, find_movies_by_years,myyear,1)
        return x
        
def func_movies_by_director (qry_func,parm1,director) :
        find_movies_by_dir = "select title, director from movies_normalized_meta, movies_normalized_director where movies_normalized_meta.ai_myid=movies_normalized_director.ai_myid and director = %s"
        x = qry_func(parm1, find_movies_by_dir,director,1)
        return x     
           
def func_movies_by_actor_with_many (qry_func,parm1,actor) :
        find_movies_by_dir = "select title, actor_name, actor_character from movies_normalized_cast, movies_normalized_actors, movies_normalized_meta where movies_normalized_meta.ai_myid=movies_normalized_cast.ai_myid and movies_normalized_actors.ai_actor_id=movies_normalized_cast.ai_actor_id and movies_normalized_actors.ai_actor_id=%s"
        x = qry_func(parm1, find_movies_by_dir,actor,1)
        return x       

def func_top_movies_year_json (qry_func,parm1,myyear) :
        find_movies_by_years = "select json_column from movies_json_generated where json_column->>'$.title' like '%s' order by  json_column->>'$.imdb_rating' desc limit 50"
        x = qry_func(parm1, find_movies_by_years,myyear,1)
        return x
        
def func_movies_by_director_json (qry_func,parm1,director) :
        find_movies_by_dir = "select json_column-->'$.title', t.myname  from movies_json_generated, json_table(json_column, '$.director[*]' columns( myid varchar(20) path '$.id', myname varchar(100) path '$.name')) t where t.myname = %s"
        x = qry_func(parm1, find_movies_by_dir,director,1)
        return x     
           
def func_movies_by_actor_with_many_json (qry_func,parm1,actor) :
        find_movies_by_dir = "select title, actor_name, actor_character from movies_normalized_cast, movies_normalized_actors, movies_normalized_meta where movies_normalized_meta.ai_myid=movies_normalized_cast.ai_myid and movies_normalized_actors.ai_actor_id=movies_normalized_cast.ai_actor_id and actor_name=%s"
        x = qry_func(parm1, find_movies_by_dir,actor,1)
        return x     

def func_find_movie_by_actor_json (qry_func,parm1,actor_name) :
        find_movies_by_actor = "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= %s and actor_name != ''"
        x = qry_func(parm1, find_movies_by_actor,actor_name,1)
        return x
        
def func_find_movies_by_title_json (qry_func,parm1,title_name) :
        find_movies_by_title = "select ai_myid, imdb_id, title, imdb_rating from movies_normalized_meta a where title = %s"
        x = qry_func(parm1, find_movies_by_title,title_name,1)
        return x
        
def func_find_movies_by_id_json (qry_func,parm1,id) :
        simulate_finding_a_movie_record = "select ai_myid, imdb_id, year, title, json_column from movies_normalized_meta where imdb_id = %s"
        x = qry_func(parm1, simulate_finding_a_movie_record,id,1)
        return x       

def func_find_actors_and_characters_by_title_json (qry_func,parm1,title_name) :
        find_movies_by_title = "select title, imdb_rating, actor_name, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and title =%s"
        x = qry_func(parm1, find_movies_by_title,title_name,1)
        return x       

def func_find_movies_by_ai_id_json (qry_func,parm1,id) :
        simulate_finding_a_movie_record = "select ai_myid, imdb_id, year, title, json_column from movies_normalized_meta where ai_myid = %s"
        x = qry_func(parm1, simulate_finding_a_movie_record,(id,),1)
        return x      

def func_log_movie_view (qry_func,parm1,MYDSN,list_ids,movie_views) :

     myconnect = new_connection(MYDSN)
     mycursor = myconnect.cursor()
     letters = string.ascii_lowercase
     my_query = query_db_new_connect
     #parm1 = MYDSN

     i = 0
     while i < movie_views :
         i = i + 1
         encoded_values = func_generate_comment(letters,20,50)
         watch_time = random.randrange(1, 5000)
         watch_user_id = random.randrange(1, 500000)
         search_id = random.choice(list_ids)
         results = func_find_movies_by_ai_id_json(my_query,MYDSN,search_id)


         mid = results[0][0]
         mjson = results[0][4]
         my_imdb = results[0][1]
         simulate_comment_on_movie = "insert into movies_viewed_logs (ai_myid, imdb_id, watched_user_id, time_watched_sec, encoded_data, json_payload) values ( %s, %s, %s, %s, %s, %s )" 
         #print(mid,my_imdb,watch_user_id,watch_time,encoded_values,mjson)
   
         mycursor.execute(simulate_comment_on_movie, (mid,my_imdb,watch_user_id,watch_time,encoded_values,json.dumps(mjson)))
     
     myconnect.commit();             
     return i;

def func_del_log_movie_view (qry_func,parm1,MYDSN,list_ids) :

     myconnect = new_connection(MYDSN)
     mycursor = myconnect.cursor()
     delete = "delete from movies_viewed_logs where watched_time < current_timestamp - interval '15 minutes'" 
     mycursor.execute(delete)
     myconnect.commit();             
     return 1;

def func_create_drop_title_index (mydsn) :
        val = func_find_index(mydsn,'movies_normalized_meta','idx_nmm_title')
        logging.info('value: ', val)
        create_index = "create index idx_nmm_title on movies_normalized_meta (title) "
        drop_index = "drop index idx_nmm_title"
        
        if val[0][0] == 0:
           logging.info('creating index on Title')
           x = query_db_new_connect(mydsn, create_index,(),0)
        else :
           logging.info('dropping index on Title') 
           x = query_db_new_connect(mydsn, drop_index,(),0)
        return x    
        
def func_create_drop_year_index (mydsn) :
        val = func_find_index(mydsn,'movies_normalized_meta','idx_nmm_year')
        logging.info('value: ', val)
        create_index = "create index idx_nmm_year on movies_normalized_meta (year) "
        drop_index = "drop index idx_nmm_year"
        
        if val[0][0] == 0:
           logging.debug('creating index on Year')
           x = query_db_new_connect(mydsn, create_index,(),0)
        else :
           logging.debug('dropping index on Year') 
           x = query_db_new_connect(mydsn, drop_index,(),0)
        return x   
        
def func_create_title_index (mydsn) :
        val = func_find_index(mydsn,'movies_normalized_meta','idx_nmm_title')
        logging.info('value: ' + str(val[0][0]))
        create_index = "create index idx_nmm_title on movies_normalized_meta (title) "
        drop_index = "drop index idx_nmm_title"
        
        if val[0][0] == 0:
           logging.debug('creating index on Title')
           x = query_db_new_connect(mydsn, create_index,(),0)
        else :
           logging.debug('Index on Title already exists') 
           #x = query_db_new_connect(mydsn, drop_index,(),0)
           x = 0
        return x   
                
def func_drop_title_index (mydsn) :
        val = func_find_index(mydsn,'movies_normalized_meta','idx_nmm_title')
        logging.info('value: ' + str(val[0][0]))
        create_index = "create index idx_nmm_title on movies_normalized_meta (title) "
        drop_index = "drop index idx_nmm_title"
        
        if val[0][0] == 0:
           logging.debug('Index on title, does not exist')
           x = 0
           #x = query_db_new_connect(mydsn, create_index,(),0)
        else :
           logging.debug('dropping index on title') 
           x = query_db_new_connect(mydsn, drop_index,(),0)
        return x   
         
def func_create_year_index (mydsn) :
        val = func_find_index(mydsn,'movies_normalized_meta','idx_nmm_year')
        logging.info('value: ' + str(val[0][0]))
        create_index = "create index idx_nmm_year on movies_normalized_meta (year) "
        drop_index = "drop index idx_nmm_year"
        
        if val[0][0] == 0:
           logging.info('creating index on Year')
           x = query_db_new_connect(mydsn, create_index,(),0)
        else :
           logging.info('Index already exsits') 
           x = 0
        return x  
          
def func_drop_year_index (mydsn) :
        val = func_find_index(mydsn,'movies_normalized_meta','idx_nmm_year')
        logging.info('value: ' + str(val[0][0]))
        create_index = "create index idx_nmm_year on movies_normalized_meta (year) "
        drop_index = "drop index idx_nmm_year"
        
        if val[0][0] == 0:
           logging.info('Index on Year does not exist')
           x = 0
        else :
           logging.info('dropping index on Year') 
           x = query_db_new_connect(mydsn, drop_index,(),0)
        return x   
        
def func_add_actor_year_index (mydsn) :  
         val = func_find_index(mydsn,'movies_normalized_actors','actor_name_idx')
         logging.info('value: ' + str(val[0][0]))
         create_index = "create index actor_name_idx on movies_normalized_actors (actor_name) "
         drop_index = "drop index actor_name_idx"
        
         if val[0][0] == 0:
           logging.info('creating index on actor name')
           x = query_db_new_connect(mydsn, create_index,(),0)
         else :
           logging.info('Actor Index already exsits') 
           x = 0
         return x
         
def func_drop_actor_year_index (mydsn) :
         val = func_find_index(mydsn,'movies_normalized_actors','actor_name_idx')
         logging.info('value: ' + str(val[0][0]))
         create_index = "create index actor_name_idx on movies_normalized_actors (actor_name) "
         drop_index = "drop index actor_name_idx"
        
         if val[0][0] == 0:
            logging.info('Index on Actor Name does not exist')
            x = 0
         else :
            logging.info('dropping index on Actor Name') 
            x = query_db_new_connect(mydsn, drop_index,(),0)
         return x        
                
def func_find_index (mydsn, table_name, index_name) :        
        findIdx = "select count(*) from  pg_class t, pg_class i, pg_index ix, pg_attribute a where  t.oid = ix.indrelid and i.oid = ix.indexrelid  and a.attrelid = t.oid  and a.attnum = ANY(ix.indkey)  and t.relkind = 'r' and t.relname like %s and i.relname=%s";        
        x = query_db_new_connect(mydsn, findIdx, (table_name, index_name), 1)
        return(x)     
        
 
def func_create_index (mydsn,name,table,columns) :
        val = func_find_index(mydsn,table,name)
        logging.info('value: ', val)
        create_index = "create index "+ name +" on "+ table +" ("+ columns +") "
        drop_index = "drop index "+ name 
        
        if val[0][0] == 0:
           logging.info('creating index on %s : %s', table, columns)
           x = query_db_new_connect(config, create_index,(),0)
        else :
           logging.info('Index on %s : %s already exsits', table, columns)
           x = 0
        return x  
          
def func_drop_index (mydsn,name,table,columns) :
        val = func_find_index(mydsn,table,name)
        logging.info('value: ', val)
        create_index = "create index "+ name +" on "+ table +" ("+ columns +") "
        drop_index = "drop index "+ name
        
        if val[0][0] == 0:
           logging.info('Index on %s : %s does not exsit', table, columns)
           x = 0
        else :
           logging.info('dropping index on %s : %s', table, drop_index) 
           x = query_db_new_connect(config, drop_index,(),0)
        return x   

def func_toggle_index (mydsn,name,table,columns) :
        val = func_find_index(mydsn,table,name)
        logging.info('value: ', val)
        create_index = "create index "+ name +" on "+ table +" ("+ columns +") "
        drop_index = "drop index "+ name
        
        if val[0][0] == 0:
           logging.info('creating index on %s : %s', table, columns)
           x = query_db_new_connect(config, create_index,(),0)
        else :
           logging.info('dropping index on %s : %s', table, drop_index) 
           x = query_db_new_connect(config, drop_index,(),0)
        return x  

def reset_optimized_indexes(config):
          func_create_index(config,'idx_comments_id','movies_normalized_user_comments','ai_myid,comment_add_time')
          func_create_index(config,'idx_comments_com_time','movies_normalized_user_comments','comment_add_time')
          
          func_create_index(config,'idx_nmm_rate','movies_normalized_meta','imdb_rating')
          func_create_index(config,'idx_nmm_year_rate','movies_normalized_meta','year,imdb_rating')
          func_create_index(config,'idx_nmm_country_year','movies_normalized_meta','country,year,imdb_rating')
          func_create_index(config,'idx_nmm_title','movies_normalized_meta','title')
          
          func_create_index(config,'idx_nd_id','movies_normalized_director','ai_myid')
          func_create_index(config,'idx_nd_director','movies_normalized_director','director, ai_myid')
          
          func_create_index(config,'idx_nc_char','movies_normalized_cast','actor_character')
          func_create_index(config,'idx_nc_id2','movies_normalized_cast','ai_actor_id,ai_myid')
          func_create_index(config,'idx_nc_id','movies_normalized_cast','ai_myid')
          
          func_create_index(config,'idx_na_name','movies_normalized_actors','actor_name')
          
def drop_all_indexes(config):
          func_drop_index(config,'idx_comments_id','movies_normalized_user_comments','ai_myid,comment_add_time')
          func_drop_index(config,'idx_comments_com_time','movies_normalized_user_comments','comment_add_time')
          
          func_drop_index(config,'idx_nmm_rate','movies_normalized_meta','imdb_rating')
          func_drop_index(config,'idx_nmm_year_rate','movies_normalized_meta','year,imdb_rating')
          func_drop_index(config,'idx_nmm_country_year','movies_normalized_meta','country,year,imdb_rating')
          func_drop_index(config,'idx_nmm_title','movies_normalized_meta','title')
          
          func_drop_index(config,'idx_nd_id','movies_normalized_director','ai_myid')
          func_drop_index(config,'idx_nd_director','movies_normalized_director','director, ai_myid')
          
          func_drop_index(config,'idx_nc_char','movies_normalized_cast','actor_character')
          func_drop_index(config,'idx_nc_id2','movies_normalized_cast','ai_actor_id,ai_myid')
          func_drop_index(config,'idx_nc_id','movies_normalized_cast','ai_myid')
          
          func_drop_index(config,'idx_na_name','movies_normalized_actors','actor_name')          
          
def reset_mixed_indexes(config):
          func_drop_index(config,'idx_nmm_year_rate','movies_normalized_meta','year,imdb_rating')
          func_drop_index(config,'idx_nd_director','movies_normalized_director','director, ai_myid')
          func_drop_index(config,'idx_nmm_country_year','movies_normalized_meta','country,year,imdb_rating')
                                    
