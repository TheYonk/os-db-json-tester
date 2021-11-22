import time 
import random
import argparse
import threading
import logging
import mysql.connector
import string
import os

config = {
  'user': 'movie_json_user',
  'password': 'Ch@nge_me_1st',
  'host': '127.0.0.1',
  'database': 'movie_json_test',
  'raise_on_warnings': True
}

def load_db(config): 
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

    #config = pg_return_dsn()
    cnx = mysql.connector.connect(**config)
    
    #cnx = mysql.connector.connect(**config)
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
    #func_fill_random_comments(config,ai_myids,100000)
    
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
      cursor = cnx.cursor(buffered=True)
      cursor.execute(sql, parms)      
      if fetch:
         x = cursor.fetchall()
      cnx.commit();
      return(x)

def query_db_new_connect(config,sql,parms,fetch):
      x=[]
      cnx = mysql.connector.connect(**config)
      cursor = cnx.cursor(buffered=True)
      try:
        if parms == ():
           cursor.execute(sql)
        else :
           cursor.execute(sql, parms)
      except mysql.connector.Error as err:
        print("problem with general new connect_query")
        print("Something went wrong: {}".format(err))              
        print(sql, parms)
        pass
        
      if fetch:
         x = cursor.fetchall()
      cnx.commit()
      cnx.close()
      return(x)

def new_connection(config):
    cnx = mysql.connector.connect(**config)
    return cnx
      
      
def single_user_actions_v2(config,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,activelist1):
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
 
    while activelist1[os.getpid()] == 1 :         
         current_time = time.perf_counter() - start_time    
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
        x = 0 
        try:
          x = qry_func(parm1, simulate_updating_movie_record_with_vote,myparm,0)
        except mysql.connector.Error as err:
          print("problem with long running select update on vote")
          print("Something went wrong: {}".format(err))              
          print(upvote, id)

          pass
        return x     

def func_find_up_down_votes (qry_func,parm1,id ) :
        simulate_updating_movie_record_with_vote = "select ai_myid, imdb_id, title, upvotes, downvotes from  movies_normalized_meta where imdb_id = %s"
        x = qry_func(parm1, simulate_updating_movie_record_with_vote,id,1)
        return x    
        
def func_find_movie_comments (qry_func,parm1,id ) :
        query = "select comment_id, ai_myid, comment, comment_add_time, comment_update_time from movies_normalized_user_comments where ai_myid = %s order by comment_add_time desc"
        x = qry_func(parm1, query,(id,),1)
        return x    

def func_create_drop_title_index (config) :
        val = func_find_index(config,'movies_normalized_meta','idx_nmm_title')
        print('value: ', val)
        create_index = "create index idx_nmm_title on movies_normalized_meta (title) "
        drop_index = "drop index idx_nmm_title on movies_normalized_meta"
        
        if val[0][0] == 0:
           logging.debug('creating index on Title')
           x = query_db_new_connect(config, create_index,(),0)
        else :
           logging.debug('dropping index on Title') 
           x = query_db_new_connect(config, drop_index,(),0)
        return x    
        
def func_create_title_index (config) :
        val = func_find_index(config,'movies_normalized_meta','idx_nmm_title')
        print('value: ', val)
        create_index = "create index idx_nmm_title on movies_normalized_meta (title) "
        drop_index = "drop index idx_nmm_title on movies_normalized_meta"
        
        if val[0][0] == 0:
           logging.debug('creating index on Title')
           x = query_db_new_connect(config, create_index,(),0)
        else :
           logging.debug('Index on Title already exists') 
           #x = query_db_new_connect(config, drop_index,(),0)
           x = 0
        return x    

def func_drop_title_index (config) :
        val = func_find_index(config,'movies_normalized_meta','idx_nmm_title')
        print('value: ', val)
        create_index = "create index idx_nmm_title on movies_normalized_meta (title) "
        drop_index = "drop index idx_nmm_title on movies_normalized_meta"
        
        if val[0][0] == 0:
           logging.debug('Index on Title does not exist')
           #x = query_db_new_connect(config, create_index,(),0)
           x = 0
        else :
           logging.debug('dropping index on Title') 
           x = query_db_new_connect(config, drop_index,(),0)
        return x    
        
def func_create_drop_year_index (config) :
        val = func_find_index(config,'movies_normalized_meta','idx_nmm_year')
        print('value: ', val)
        create_index = "create index idx_nmm_year on movies_normalized_meta (year) "
        drop_index = "drop index idx_nmm_year on movies_normalized_meta"
        
        if val[0][0] == 0:
           print('creating index on Year')
           x = query_db_new_connect(config, create_index,(),0)
        else :
           print('dropping index on Year') 
           x = query_db_new_connect(config, drop_index,(),0)
        return x    

def func_create_year_index (config) :
        val = func_find_index(config,'movies_normalized_meta','idx_nmm_year')
        print('value: ', val)
        create_index = "create index idx_nmm_year on movies_normalized_meta (year) "
        drop_index = "drop index idx_nmm_year"
        
        if val[0][0] == 0:
           print('creating index on Year')
           x = query_db_new_connect(config, create_index,(),0)
        else :
           print('Index already exsits') 
           x = 0
        return x  
          
def func_drop_year_index (config) :
        val = func_find_index(config,'movies_normalized_meta','idx_nmm_year')
        print('value: ', val)
        create_index = "create index idx_nmm_year on movies_normalized_meta (year) "
        drop_index = "drop index idx_nmm_year"
        
        if val[0][0] == 0:
           print('Index on Year does not exist')
           x = 0
        else :
           print('dropping index on Year') 
           x = query_db_new_connect(config, drop_index,(),0)
        return x   
        
                
def func_find_index (config, table_name, index_name) :        
        findIdx = "select count(*) from information_schema.statistics where table_name = %s and index_name = %s;";        
        x = query_db_new_connect(config, findIdx, (table_name, index_name), 1)
        return(x)

def func_create_materialized_view_votes(config) :
    
    createmtlv = "CREATE TABLE if not exists view_voting_counts (ai_myid int DEFAULT NULL,  title varchar(255) DEFAULT NULL, imdb_id varchar(20) DEFAULT NULL,  comment_count int NOT NULL DEFAULT '0',  max_rate decimal(3,2) DEFAULT NULL, avg_rate decimal(14,4) DEFAULT NULL, upvotes decimal(32,0) DEFAULT NULL, downvotes decimal(32,0) DEFAULT NULL ) ENGINE=InnoDB"; 
    x = query_db_new_connect(config, createmtlv, (), 0)
    return(x)
    
def func_refreshed_materialized_view_votes(config) :
    myconnect = new_connection(config)
    mycursor = myconnect.cursor()
    createmtlv = "delete from view_voting_counts";        
    x = mycursor.execute(createmtlv)
    createmtlv = "insert into view_voting_counts select a.ai_myid, title, a.imdb_id, count(*) as comment_count, max(rating) as max_rate, avg(rating) as avg_rate, sum(upvotes) as upvotes, sum(downvotes) as downvotes from movies_normalized_user_comments a join movies_normalized_meta b on a.ai_myid=b.ai_myid  group by a.ai_myid, title, a.imdb_id";               
    x = mycursor.execute(createmtlv)
    myconnect.commit()
    myconnect.close()
    return(x)
    
def func_create_materialized_view_movies_by_year(config)  :
    createmtlv = "CREATE TABLE if not exists  view_year_counts ( year int DEFAULT NULL, movie_count int NOT NULL DEFAULT '0',  avg_imdb_rating decimal(4,2) DEFAULT NULL,  max_rate decimal(4,2) DEFAULT NULL,  avg_rate decimal(14,4) DEFAULT NULL,  upvotes decimal(32,0) DEFAULT NULL,  downvotes decimal(32,0) DEFAULT NULL) ENGINE=InnoDB";  
    x = query_db_new_connect(config, createmtlv, (), 0)
    return(x)
    
def func_refreshed_materialized_view_movies_by_year(config)  :            
    myconnect = new_connection(config)
    mycursor = myconnect.cursor()
    createmtlv = "delete from view_year_counts";        
    x = mycursor.execute(createmtlv)
    createmtlv = "insert into view_year_counts select year, count(distinct b.imdb_id) as movie_count, avg(imdb_rating) as avg_imdb_rating,  max(rating) as max_rate, avg(rating) as avg_rate, sum(upvotes) as upvotes, sum(downvotes) as downvotes from movies_normalized_user_comments a join movies_normalized_meta b on a.ai_myid=b.ai_myid  group by year";        
    x = mycursor.execute(createmtlv)
    myconnect.commit()
    myconnect.close()
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
                              
def func_find_movie_by_actor_for_update (config,actor_name, sleeptime) :
        myconnect = new_connection(config)
        mycursor = myconnect.cursor(buffered=True)
        find_movies_by_actor = "select title, imdb_rating, actor_character from movies_normalized_meta a, movies_normalized_cast b,  movies_normalized_actors c where a.ai_myid=b.ai_myid and b.ai_actor_id = c.ai_actor_id and actor_name= %s and actor_name != '' for update"
        mycursor.execute(find_movies_by_actor, actor_name)
        x = mycursor.fetchall()
        time.sleep(sleeptime)
        myconnect.commit()
        return x
        
def func_really_bad_for_update (config, sleeptime, year1, year2) :
        myconnect = new_connection(config)
        mycursor = myconnect.cursor(buffered=True)
        query = "select ai_myid, title, imdb_rating from movies_normalized_meta where year > %s and year < %s order by imdb_rating desc limit 25 for update  "
        mycursor.execute(query, (year1,year2))
        x = mycursor.fetchall()
        time.sleep(sleeptime)
        myconnect.commit()
        return x        

def func_find_update_meta (config, sleeptime, title) :
        myconnect = new_connection(config)
        mycursor = myconnect.cursor(buffered=True)
        title2 = title[0][0:3]+'%'
        query = "select ai_myid,title, imdb_rating, year from movies_normalized_meta where title like %s for update  "
        update = " update movies_normalized_meta set upvotes = upvotes + 1, imdb_rating = imdb_rating * 1, year = year where ai_myid = %s  "
        
        try:
          mycursor.execute(query, (title2,))
          x = mycursor.fetchall()
        except mysql.connector.Error as err:
          print("problem with long running select update on meta")
          print("Something went wrong: {}".format(err))              
          print(update, y[0])
          pass
        except:
          print("problem with update LT, non-mysql,func_find_update_meta 1")
          print(update, y[0])
        time.sleep(sleeptime)
        y = random.choice(x)
        try:
          mycursor.execute(update, (y[0],))
          x = mycursor.fetchall()
        except mysql.connector.Error as err:
          print("problem with long running update on meta")
          print("Something went wrong: {}".format(err))              
          print(update, y[0])
          pass
        except:
          print("problem with update LT, non-mysql,func_find_update_meta 2 ")
          print(update, y[0])
          
        time.sleep(sleeptime)
        myconnect.commit()
        return x   
             
def func_find_update_comments (config,sleeptime) :
        myconnect = new_connection(config)
        mycursor = myconnect.cursor(buffered=True)
        query = "select comment_id, ai_myid, comment_add_time, comment_update_time from movies_normalized_user_comments order by comment_add_time desc limit 25 for update"
        update = "update movies_normalized_user_comments set comment = comment, rating = rating * 1, comment_update_time = current_timestamp where comment_id = %s"
        delete = "delete from movies_normalized_user_comments where comment_id = %s"
        mycursor.execute(query, ())
        x = mycursor.fetchall()
        time.sleep(sleeptime)
        if(len(x) >0):
          y = x.pop()
          try:
            mycursor.execute(update, (y[0],))
          except mysql.connector.Error as err:
            print("problem with long running update")
            print("Something went wrong: {}".format(err))              
            print(update, y[0])
            pass
          except:
            print("problem with update LT, non-mysql, func_find_update_comments 1")
        if(len(x) >0):
          y = x.pop()
          try:
            mycursor.execute(update, (y[0],))
          except mysql.connector.Error as err:
            print("problem with long running update")
            print("Something went wrong: {}".format(err))              
            print(update, y[0])
            pass
          except:
            print("problem with update LT, non-mysql, func_find_update_comments 2")
        if(len(x) >0):
          y = x.pop()
          try:
            mycursor.execute(delete, (y[0],))
          except mysql.connector.Error as err:
            print("problem with long running delete")
            print("Something went wrong: {}".format(err))              
            print(delete, y[0])
            pass
          except:
            print("problem with update LT, non-mysql, func_find_update_comments 3")
        time.sleep(sleeptime/2)
        myconnect.commit()
        myconnect.close()
        return x        

def func_fill_random_comments (config,list_ids,comments_to_add) :
    
     myconnect = new_connection(config)
     mycursor = myconnect.cursor(buffered=True)
     letters = string.ascii_lowercase
     i = 0
     while i < comments_to_add :
         i = i + 1
         comment = func_generate_comment(letters,20,50)
         vote = func_generate_vote()
         search_id = random.choice(list_ids)
         simulate_comment_on_movie = "insert into movies_normalized_user_comments (ai_myid, rating, comment ) values ( %s, %s, %s )" 
         mycursor.execute(simulate_comment_on_movie, (search_id,vote['uservote'], comment))
     myconnect.commit()
     myconnect.close()
     return i;

def func_delete_random_comments (config,list_ids,comments_to_remove) :
    
     myconnect = new_connection(config)
     mycursor = myconnect.cursor(buffered=True)
     mycursor2 = myconnect.cursor(buffered=True)
     
     letters = string.ascii_lowercase
     i = 0
     while i < comments_to_remove :
         i = i + 1
         search_id = random.choice(list_ids)
         select = "select comment_id from movies_normalized_user_comments where ai_myid = %s limit 3"
         mycursor.execute(select,(search_id,))
         for (comment_id) in mycursor:
             delete = "delete from movies_normalized_user_comments where comment_id = %s" 
             try:
               mycursor2.execute(delete, comment_id)
             except mysql.connector.Error as err:
               print("problem with delete")
               print("Something went wrong: {}".format(err))              
               print(delete, comment_id)
               pass
             except:
               print("problem with delete, non-mysql")
               break
               
     myconnect.commit();             
     return i;


def func_update_random_comments (config,list_ids,comments_to_update) :
        myconnect = new_connection(config)
        mycursor = myconnect.cursor(buffered=True)
        i = 0
        while i < comments_to_update :
            i = i + 1
            search_id = random.choice(list_ids)
            update = "update movies_normalized_user_comments set comment = comment, rating = rating * 1, comment_update_time = current_timestamp where ai_myid = %s"
            try:
              mycursor.execute(update, (search_id,))
            except mysql.connector.Error as err:
              print("problem with Update")
              print("Something went wrong: {}".format(err))              
              print(update, search_id)
              pass
            except:
              print("problem with Update, non-mysql")
              break
        myconnect.commit();             
        return i;
        
     
def run_vacuum(config):
     myconnect = new_connection(config)
     mycursor = myconnect.cursor(buffered=True)
    
     try:
       query = "optimize table movies_normalized_meta"
       x = mycursor.execute(query, ())
       query = "optimize table movies_normalized_user_comments"
       x = mycursor.execute(query, ())
     except mysql.connector.Error as err:
       print("problem with Update")
       print("Something went wrong: {}".format(err))              
       print(update, search_id)
     
def make_copy_of_table (config):
     query = "DROP TABLE IF EXISTS copy_of_json_table"
     x = query_db_new_connect(config, query, (), 0)
     
     query = "create table copy_of_json_table ( ai_myid int AUTO_INCREMENT primary key, imdb_id varchar(255) generated always as (`json_column` ->> '$.imdb_id'),	title varchar(255) generated always as (`json_column` ->> '$.title'),imdb_rating decimal(5,2) generated always as (json_value(json_column, '$.imdb_rating')) , overview text generated always as (`json_column` ->> '$.overview'),  director json generated always as (`json_column` ->> '$.director'), country varchar(100) generated always as (`json_column` ->> '$.country'), json_column json, key(title), key(imdb_id) ) engine = innodb"
     x = query_db_new_connect(config, query, (), 0)
     
     query = "insert into copy_of_json_table (json_column) select json_column from movies_json_generated"
     x = query_db_new_connect(config, query, (), 0)
     

def refresh_all_mat_views (config):
    
    x = func_create_materialized_view_votes(config)
    x = func_refreshed_materialized_view_votes(config)
    x = func_create_materialized_view_movies_by_year(config)
    x = func_refreshed_materialized_view_movies_by_year(config)
    
    
def func_pk_bigint (config) :
            alter_pk = "alter table movies_normalized_meta modify column ai_myid bigint AUTO_INCREMENT "
            print('Changing PK to  bigint') 
            x = query_db_new_connect(config, alter_pk,(),0)
            print('done Changing PK') 
            
            return x

def func_pk_int (config) :
            alter_pk = "alter table movies_normalized_meta modify column ai_myid int AUTO_INCREMENT"
            print('Changing PK to  int') 
            x = query_db_new_connect(config, alter_pk,(),0)
            print('done Changing PK') 
            
            return x
            
def func_pk_varchar (config) :
            alter_pk = "alter table movies_normalized_meta modify column ai_myid varchar(32) "
            print('Changing PK to  Varchar') 
            x = query_db_new_connect(config, alter_pk,(),0)
            print('done Changing PK') 
            
            return x
        
def report_user_actions(config,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,activelist2):
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    error = 0 
    
    print("pid: " , os.getpid())
    print("Active List: " , str(activelist2))
   
    
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
 
    while activelist2[os.getpid()] == 1 : 
         if error == 1 :
            print('Starting Over! ', os.getpid())
            error = 0        
         current_time = time.perf_counter() - start_time    
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = mysql.connector.connect(**config)
                
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
#    threadlist.append(threading.Thread(target=single_user_actions_v2, args=(config,args.time_to_run,args.sleep_timer,args.create_new_connection,args.create_new_connection_per_qry,list_actors,list_tiles,list_ids)))
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

def insert_update_delete(config,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,activelist3):
    
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    error = 0
    print("pid: " , os.getpid())
    print("Active List: " , str(activelist3))
   
    
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
 
    while activelist3[os.getpid()] == 1 :         
         current_time = time.perf_counter() - start_time   
         if error == 1 :
             print('Starting Over! ', os.getpid())
             error = 0 
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = mysql.connector.connect(**config)
                
         search_actor = random.choice(list_actors)
         search_title = random.choice(list_tiles)
         search_id = random.choice(ai_myids)
         
         try: 
           x = func_fill_random_comments(config,ai_myids,10) 
           x = func_update_random_comments(config, ai_myids, 5) 
           x = func_delete_random_comments(config,ai_myids,6) 
           time.sleep(sleep_timer)
         except: 
           print("problem in the insert update delete part 1")
           print('pid:', os.getpid())
           error = 1
           pass
         
         try: 
            r = random.randint(1,3)
            if r == 1:
              x = func_fill_random_comments(config,ai_myids,5)
            if r == 2:
              x = func_delete_random_comments(config,ai_myids,2)
            if r == 3 :
              x = func_update_random_comments(config, ai_myids, 2) 
         except: 
           print("problem in the insert update delete part 2")
           print('pid:', os.getpid())
           error = 1
           pass
         time.sleep(sleep_timer)
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    print("Ending Loop....")
    print("Started at..." + str( start_time))
    print("Ended at..." + str( time.perf_counter()))
    del activelist3[os.getpid()]
    exit()
    

def long_transactions(config,time_to_run,sleep_timer,create_new_connection,create_new_connection_per_qry,list_actors,list_tiles,list_ids,ai_myids,activelist4):
    
    current_time = 0
    start_time = 0
    qry = 0
    debug = 0
    error = 0
    print("pid: " , os.getpid())
    print("Active List: " , str(activelist4))
   
    
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
 
    while activelist4[os.getpid()] == 1 :         
         if error == 1 :
             print('Starting Over! ', os.getpid())
             error = 0
         current_time = time.perf_counter() - start_time    
         if create_new_connection : 
              parm1.commit()  
              parm1.close()
              my_query = query_db
              parm1 = mysql.connector.connect(**config)
                
         search_actor = random.choice(list_actors)
         search_title = random.choice(list_tiles)
         search_id = random.choice(list_ids)
         year1 = random.randrange(1900, 2015, 5)
         year2 =  year1 + 10	
         locktime = (random.randint(50,1000)/100) 
         
         try: 
           r = random.randint(1,4)
           if r == 1:
             x = func_really_bad_for_update (config, locktime, year1, year2)
           if r == 2:
              x = func_find_movie_by_actor_for_update (config,search_actor,locktime)
           if r == 3 :
              x = func_find_update_meta (config, locktime, search_title) 
           if r == 4 :
              x = func_find_update_comments (config,locktime)
         except: 
           print("error in long running transaction")
           print('pid:', os.getpid())
           error = 1
         
 
    if not create_new_connection_per_qry :
        parm1.commit()
    print("Ending Loop....")
    print("Started at..." + str( start_time))
    print("Ended at..." + str( time.perf_counter()))
    del activelist4[os.getpid()]
    exit()
