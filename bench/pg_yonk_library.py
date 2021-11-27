import time 
import random
import argparse
import threading
import logging
import psycopg2
import string
import os


def load_db(MYDSN): 
    print ('inside load function')
    
    load_ids = "select ai_myid, imdb_id, year from movies_normalized_meta"
    load_actors = "select actor_name from movies_normalized_actors where actor_name != ''"
    load_titles = "select title, imdb_id, year from movies_normalized_meta"
    load_directors = "select director from movies_normalized_director"
    comments_cleanup = "truncate movies_normalized_user_comments"

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
        
    print("Loading Actors...")

    #cursor.execute(comments_cleanup)
    #cnx.commit()
    #func_fill_random_comments(MYDSN,ai_myids,100000)
    
    cursor.execute(load_actors)

    
    for actor_name in cursor:
            list_actors.append(actor_name);

    print("Loading Titles & ID's...")

    cursor.execute(load_titles)
    for (movie_titles, imdb_id, mv_year) in cursor:
            list_titles.append(movie_titles);
            list_ids.append(imdb_id);
            movie_years[imdb_id] = mv_year
    
    returnval['list_actors'] = list_actors
    returnval['list_titles'] = list_titles
    returnval['list_ids'] = list_ids
    returnval['ai_myids'] = ai_myids
    
    return returnval

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

def new_connection(MYDSN):
    connect = psycopg2.connect(MYDSN)  
    return connect
      
      
def single_user_actions_v2(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,activelist1):
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    
    print("pid: " , os.getpid())
    print("Active List: " , str(activelist1))
    
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
 
    while activelist1[os.getpid()] == 1 :         
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

         myid = x[0][0]
         
         x = func_find_movie_comments (my_query,parm1,myid)
         
         vote = func_generate_vote()
         #print(str(vote))
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
    if not create_new_connection_per_qry :
        parm1.commit()
    print("Ending Loop....")
    print("Started at..." + str( start_time))
    print("Ended at..." + str( time.perf_counter()))
    del activelist1[os.getpid()]
    exit()
    
def single_user_actions_solo(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids):
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
 
    while runme == 1 :         
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

         myid = x[0][0]
         
         x = func_find_movie_comments (my_query,parm1,myid)
         
         vote = func_generate_vote()
         #print(str(vote))
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
        
def func_find_movie_comments (qry_func,parm1,id ) :
        query = "select comment_id, ai_myid, comment, comment_add_time, comment_update_time from movies_normalized_user_comments where ai_myid = %s order by comment_add_time desc"
        x = qry_func(parm1, query,(id,),1)
        return x    

def func_create_drop_title_index (mydsn) :
        val = func_find_index(mydsn,'movies_normalized_meta','idx_nmm_title')
        print('value: ', val)
        create_index = "create index idx_nmm_title on movies_normalized_meta (title) "
        drop_index = "drop index idx_nmm_title"
        
        if val[0][0] == 0:
           print('creating index on Title')
           x = query_db_new_connect(mydsn, create_index,(),0)
        else :
           print('dropping index on Title') 
           x = query_db_new_connect(mydsn, drop_index,(),0)
        return x    
        
def func_create_drop_year_index (mydsn) :
        val = func_find_index(mydsn,'movies_normalized_meta','idx_nmm_year')
        print('value: ', val)
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
        print('value: ', val)
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
        print('value: ', val)
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
        print('value: ', val)
        create_index = "create index idx_nmm_year on movies_normalized_meta (year) "
        drop_index = "drop index idx_nmm_year"
        
        if val[0][0] == 0:
           print('creating index on Year')
           x = query_db_new_connect(mydsn, create_index,(),0)
        else :
           print('Index already exsits') 
           x = 0
        return x  
          
def func_drop_year_index (mydsn) :
        val = func_find_index(mydsn,'movies_normalized_meta','idx_nmm_year')
        print('value: ', val)
        create_index = "create index idx_nmm_year on movies_normalized_meta (year) "
        drop_index = "drop index idx_nmm_year"
        
        if val[0][0] == 0:
           print('Index on Year does not exist')
           x = 0
        else :
           print('dropping index on Year') 
           x = query_db_new_connect(mydsn, drop_index,(),0)
        return x   
        
def func_add_actor_year_index (mydsn) :  
         val = func_find_index(mydsn,'movies_normalized_actors','actor_name_idx')
         print('value: ', val)
         create_index = "create index actor_name_idx on movies_normalized_actors (actor_name) "
         drop_index = "drop index actor_name_idx"
        
         if val[0][0] == 0:
           print('creating index on actor name')
           x = query_db_new_connect(mydsn, create_index,(),0)
         else :
           print('Actor Index already exsits') 
           x = 0
         return x
         
def func_drop_actor_year_index (mydsn) :
         val = func_find_index(mydsn,'movies_normalized_actors','actor_name_idx')
         print('value: ', val)
         create_index = "create index actor_name_idx on movies_normalized_actors (actor_name) "
         drop_index = "drop index actor_name_idx"
        
         if val[0][0] == 0:
            print('Index on Actor Name does not exist')
            x = 0
         else :
            print('dropping index on Actor Name') 
            x = query_db_new_connect(mydsn, drop_index,(),0)
         return x        
                
def func_find_index (mydsn, table_name, index_name) :        
        findIdx = "select count(*) from  pg_class t, pg_class i, pg_index ix, pg_attribute a where  t.oid = ix.indrelid and i.oid = ix.indexrelid  and a.attrelid = t.oid  and a.attnum = ANY(ix.indkey)  and t.relkind = 'r' and t.relname like %s and i.relname=%s";        
        x = query_db_new_connect(mydsn, findIdx, (table_name, index_name), 1)
        return(x)

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
         #print('actor:', str(actor))
         actor_movie_count_by_year_range = "select actor_name, year, count(*), avg(imdb_rating) from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and year between %s and %s group by year, actor_name"
         x = qry_func(parm1, actor_movie_count_by_year_range,(year1,year2),1)
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
         #print('delete id: ',search_id)
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
            print('Changing PK to  bigint') 
            x = query_db_new_connect(MYDSN, alter_pk,(),0)
            
            logging.debug('Rebuild Mat Views, refresh')
            
            refresh_all_mat_views(MYDSN)
            
            print('done Changing PK') 
            
            return x

def func_pk_int (MYDSN) :
            logging.debug('Dropping Mat View view_year_counts')
            
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_year_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
            
            logging.debug('Dropping Mat View view_voting_counts')
            
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_voting_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
                
            alter_pk = "alter table movies_normalized_meta alter column ai_myid type int using ai_myid::int"
            print('Changing PK to  int') 
            x = query_db_new_connect(MYDSN, alter_pk,(),0)
            
            logging.debug('Rebuild Mat Views, refresh')
            
            refresh_all_mat_views(MYDSN)
            print('done Changing PK') 
            
            return x
            
def func_pk_varchar (MYDSN) :
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_year_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
            
            cleanup = "drop MATERIALIZED VIEW IF EXISTS view_voting_counts"
            x = query_db_new_connect(MYDSN, cleanup,(),0)
                
            alter_pk = "alter table movies_normalized_meta alter column ai_myid type varchar(32) using ai_myid::varchar "
            print('Changing PK to  Varchar') 
            x = query_db_new_connect(MYDSN, alter_pk,(),0)
            refresh_all_mat_views(MYDSN)
            print('done Changing PK') 
            
            return x
                    
def report_user_actions(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,activelist2):
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    
    print("pid: " , os.getpid())
    print("Active List: " , str(activelist2))
   
    
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
 
    while activelist2[os.getpid()] == 1 :         
         current_time = time.perf_counter() - start_time    
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
         
    if not create_new_connection_per_qry :
        parm1.commit()
    print("Ending Loop....")
    print("Started at..." + str( start_time))
    print("Ended at..." + str( time.perf_counter()))
    del activelist2[os.getpid()]
    exit()
       
#cnt = 0 
#threadcount = 0
#threadlist = []

#while cnt <  args.user_count:   
#    threadlist.append(threading.Thread(target=single_user_actions_v2, args=(MYDSN,args.time_to_run,args.sleep_timer,args.create_new_connection,args.create_new_connection_per_qry,list_actors,list_tiles,list_ids)))
#    threadcount = threadcount + 1    
#    threadlist[-1].start()
#    cnt = cnt + 1
#    time.sleep(args.sleep_timer)
#if len(sys.argv) == 1:
#     time_to_run = 300
#else :
#     print ("Run Time Set to: " + sys.argv[1])
#     time_to_run = int(sys.argv[1])
#current_time = 0
#debug = 0
#qry = 0;

def insert_update_delete(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,activelist3):
    
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    print("pid: " , os.getpid())
    print("Active List: " , str(activelist3))
   
    
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
 
    while activelist3[os.getpid()] == 1 :         
         current_time = time.perf_counter() - start_time    
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

         time.sleep(sleep_timer)
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    print("Ending Loop....")
    print("Started at..." + str( start_time))
    print("Ended at..." + str( time.perf_counter()))
    del activelist3[os.getpid()]
    exit()
    

def long_transactions(MYDSN,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,activelist4):
    
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    print("pid: " , os.getpid())
    print("Active List: " , str(activelist4))
   
    
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
 
    while activelist4[os.getpid()] == 1 :         
         current_time = time.perf_counter() - start_time    
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
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    print("Ending Loop....")
    print("Started at..." + str( start_time))
    print("Ended at..." + str( time.perf_counter()))
    del activelist4[os.getpid()]
    exit()