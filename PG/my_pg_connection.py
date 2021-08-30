import psycopg2
import time
import json
from psycopg2.extensions import AsIs

MYDSN = "dbname=movie_json_test user=movie_json_user password=Change_me_1st! host=127.0.0.1";
cnx = psycopg2.connect(MYDSN)
cnx2 = psycopg2.connect(MYDSN)
  
def pg_get_read_only_cursor():
  cnx.set_session(readonly=True, autocommit=True)
  return cnx.cursor()
  
def pg_get_cursor():
  return cnx2.cursor() 

def pg_commit():
   cnx2.commit()
   return 1; 
 
def benchmark_query_simple(query,iterations,description,printdetails):   
  # a simple benchmark that will loop a query X times and output the timings    
  cursor = cnx.cursor()    
  current_iteration = 0
  qry_iteration = []
  if printdetails > 0:
     print("+"*80) 
     print("#"*40)
     print("Starting test of the following: ")
     print("Query: " + query)
     print("Iterations: " + str(iterations))
     print("Description: " + str(description))
     print("#"*40)
     print("")
  overall_start_time = time.perf_counter()
  while current_iteration < iterations : 
    start_time = time.perf_counter()  
    cursor.execute(query)
    junk = cursor.fetchall()		
    if printdetails > 1:
      print("Current Iteration: " + str(current_iteration))
    qry_iteration.append(time.perf_counter() - start_time)
    current_iteration = current_iteration + 1
  total_time = time.perf_counter() - overall_start_time
  average_time = round(total_time/current_iteration,4)
  if printdetails > 0:
     print("#"*40)
     print("End test of the following: ")
     print("Query: " + query)
     print("Iterations: " + str(current_iteration))
     print("Total Time: " + str(total_time))
     print("Average Time: " + str(average_time))
     print("Min Time: " + str(min(qry_iteration)))
     print("Max Time: " + str(max(qry_iteration)))
     print("#"*40)
     print("+"*80)
     print("")
     print("")
     
  return [current_iteration,total_time,average_time,qry_iteration]  

def benchmark_query_parms_list(query,iterations,description,printdetails,parms):   
    # a benchmark that will take a parameter list and iterate through it x times
    # need parms as a array you wish to use.  
    
  cursor = cnx.cursor()    
  current_iteration = 0
  qry_iteration = [0]*iterations
  qry_indivisual_total_time = [0]*len(parms)
  qry_indivisual_total_count = [0]*len(parms)
  qry_ind_iteration = [[0 for i in range(iterations)] for j in range(len(parms))]  
  
  
  qry_count = 0
  if printdetails > 0:
     print("+"*80) 
     print("#"*40)
     print("Starting test of the following: ")
     print("Query: " + query)
     print("Parms: " + str(parms))
     print("Iterations: " + str(iterations))
     print("Description: " + str(description))
     print("#"*40)
     print("")
      
  overall_start_time = time.perf_counter()
  while current_iteration < iterations : 
    parm_id = 0
    for myparms in parms:
      if printdetails > 1:
          print("Current Iteration: " + str(current_iteration))
          print("iterations Parms: " + str(myparms))
          print("Running Query: " + str(cursor.mogrify(query,myparms)))
          
      start_time = time.perf_counter()  
      cursor.execute(query, myparms)
      junk = cursor.fetchall()
      qry_indivisual_total_time[parm_id] = qry_indivisual_total_time[parm_id] + time.perf_counter() - start_time
      qry_indivisual_total_count[parm_id] = qry_indivisual_total_count[parm_id] + 1
      qry_ind_iteration[parm_id][current_iteration] = time.perf_counter() - start_time
      qry_count = qry_count + 1
      parm_id = parm_id + 1
    if printdetails > 1:
      print("Current Iteration: " + str(current_iteration))
    qry_iteration.append(time.perf_counter() - start_time)
    current_iteration = current_iteration + 1
    		
    
  total_time = time.perf_counter() - overall_start_time
  average_time = round(total_time/current_iteration,4)
  if printdetails > 0:
     print("#"*40)
     print("End test of the following: ")
     print("Query: " + query)
     print("Iterations: " + str(current_iteration))
     print("Total Time: " + str(total_time))
     print("Average Time Per Run: " + str(average_time))
     print("Total Queries Run: " + str(qry_count))
     average_time_qry = round(total_time/qry_count,4)
     print("Average Time Per Query: " + str(average_time_qry))
     
     print("-"*20)
     for i in range(len(parms)) :
        print(".. Paramter : " + str(parms[i]))
        print(".... total time for this parm : " + str(qry_indivisual_total_time[i]))
        print(".... total count for this parm : " + str(qry_indivisual_total_count[i]))
        print(".... average time per : " + str(round(qry_indivisual_total_time[i]/qry_indivisual_total_count[i],5)))   
        print(".... Min Time: " + str(min(qry_ind_iteration[i])))
        print(".... Max Time: " + str(max(qry_ind_iteration[i])))
        if printdetails > 1 :
             print(".... Dumping All Times From Queries: " + str(qry_ind_iteration))
     print("#"*40)
     print("+"*80)
     print("")
     print("")
  return [current_iteration,total_time,average_time,qry_iteration]    
  
def benchmark_query_simple_UID(query,iterations,description,printdetails):   
    # a simple benchmark that will loop a query X times and output the timings    
    cursor = cnx2.cursor()    
    current_iteration = 0
    qry_iteration = []
    if printdetails > 0:
       print("+"*80) 
       print("#"*40)
       print("Starting test of the following: ")
       print("Query: " + query)
       print("Iterations: " + str(iterations))
       print("Description: " + str(description))
       print("#"*40)
       print("")
    overall_start_time = time.perf_counter()
    while current_iteration < iterations : 
      start_time = time.perf_counter()  
      cursor.execute(query)
      junk = cursor.fetchall()		
      if printdetails > 1:
        print("Current Iteration: " + str(current_iteration))
      qry_iteration.append(time.perf_counter() - start_time)
      current_iteration = current_iteration + 1
    total_time = time.perf_counter() - overall_start_time
    average_time = round(total_time/current_iteration,4)
    if printdetails > 0:
       print("#"*40)
       print("End test of the following: ")
       print("Query: " + query)
       print("Iterations: " + str(current_iteration))
       print("Total Time: " + str(total_time))
       print("Average Time: " + str(average_time))
       print("Min Time: " + str(min(qry_iteration)))
       print("Max Time: " + str(max(qry_iteration)))
       print("#"*40)
       print("+"*80)
       print("")
       print("")
    cnx2.commit()
    return [current_iteration,total_time,average_time,qry_iteration]
    
    
def benchmark_query_parms_list_work_around(query,iterations,description,printdetails,parms):   
    # a benchmark that will take a parameter list and iterate through it x times
    # need parms as a array you wish to use.  
    # mote this is a a total hack to get the JSON @@ or @> syntax to work with parameters, I probably am missing something here ... but damned if I can find it.   
    
  cursor = cnx.cursor()    
  current_iteration = 0
  qry_iteration = [0]*iterations
  qry_indivisual_total_time = [0]*len(parms)
  qry_indivisual_total_count = [0]*len(parms)
  qry_ind_iteration = [[0 for i in range(iterations)] for j in range(len(parms))]  
  
  qry_count = 0
  if printdetails > 0:
     print("+"*80) 
     print("#"*40)
     print("Starting test of the following: ")
     print("Query: " + query)
     print("Parms: " + str(parms))
     print("Iterations: " + str(iterations))
     print("Description: " + str(description))
     print("#"*40)
     print("")
      
  overall_start_time = time.perf_counter()
  while current_iteration < iterations : 
    parm_id = 0
    for myparms in parms:
      
      pm = myparms[0][0]
      pm = str(myparms[0][0]).replace("'",r"''") 
      pm = '"' + pm.replace('"',r'\"') + '"'
      q2 = query.replace('%s',str(pm) )
      
      start_time = time.perf_counter()
      if printdetails > 1:  
        print("Running Query: " + cursor.mogrify(q2))
      cursor.execute(q2)
      junk = cursor.fetchall()
      qry_indivisual_total_time[parm_id] = qry_indivisual_total_time[parm_id] + time.perf_counter() - start_time
      qry_indivisual_total_count[parm_id] = qry_indivisual_total_count[parm_id] + 1
      mytime = time.perf_counter() - start_time
      #print("QRY TIme: " + str(mytime) + " My Parm " + str(parm_id) + " Iteration " + str(current_iteration) )
      qry_ind_iteration[parm_id][current_iteration] = mytime
      #print("Timing:  " + str(qry_ind_iteration[parm_id]))
      qry_count = qry_count + 1
      parm_id = parm_id + 1
    if printdetails > 1:
      print("Current Iteration: " + str(current_iteration))
      print(" times : " + str(qry_ind_iteration))
    qry_iteration.append(time.perf_counter() - start_time)
    current_iteration = current_iteration + 1
    		
    
  total_time = time.perf_counter() - overall_start_time
  average_time = round(total_time/current_iteration,4)
  if printdetails > 0:
     print("#"*40)
     print("End test of the following: ")
     print("Query: " + query)
     print("Iterations: " + str(current_iteration))
     print("Total Time: " + str(total_time))
     print("Average Time Per Run: " + str(average_time))
     print("Total Queries Run: " + str(qry_count))
     average_time_qry = round(total_time/qry_count,4)
     print("Average Time Per Query: " + str(average_time_qry))
     
     print("-"*20)
     for i in range(len(parms)) :
        print(".. Paramter : " + str(parms[i]))
        print(".... total time for this parm : " + str(qry_indivisual_total_time[i]))
        print(".... total count for this parm : " + str(qry_indivisual_total_count[i]))
        print(".... average time per : " + str(round(qry_indivisual_total_time[i]/qry_indivisual_total_count[i],5)))   
        print(".... Min Time: " + str(min(qry_ind_iteration[i])))
        print(".... Max Time: " + str(max(qry_ind_iteration[i])))
        if printdetails > 1 :
             print(".... Dumping All Times From Queries: " + str(qry_ind_iteration))
     print("#"*40)
     print("+"*80)
     print("")
     print("")
  return [current_iteration,total_time,average_time,qry_iteration]       