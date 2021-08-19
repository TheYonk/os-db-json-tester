
Reqs:  MySQL or PG Setup and working.  Python installed with mysql.connector installed for MySQL, and psycopg2 for PostgreSQL

Note:  A lot of code here could be optimized, it is purposely more verbose and broken out then it needs to be.  Part of this is to allow users to try, edit, and figure it out.   I am doing something I would not recommend for production or if building a tool.  For instance I have different python scripts for each workload, with a ton of repeated code... this is inefficient and would be hard to maintain, but for the purpose of learning I find it easier to see repeated code as running and exploring each individual peice.  


For Testing MySQL:
==========================================

Step 1:  create database using MySQL/mysql_json_create_tables.sql (Change the password for the user created to what you want )
Step 2:  get your data from movienet.site, (  wget https://download.openmmlab.com/datasets/movienet/meta.v1.zip ), unpack meta.v1.zip into the meta/ directory
Step 3:  update passwords in scripts under MySQL/* as needed, ensure python is setup along with the mysql connector
Step 4:  run "python3 mysql_load_tables.py"

Now you should have a full database 

You can start running the examples in the blog.  

===========================================



If you are interested in simulated workloads:

===========================================

If you would like to run some simulated workload for testing/benchmarking you can run the following (Note this is very basic and designed to be used as a way to see some workload in tools like PMM, and compare timings of different queries, etc.  )

Optional: Install PMM based on instructions here: https://www.percona.com/software/pmm/quickstart
Note:  PMM will allow you to look at the workload, see query bottlenecks, watch the performance as these run.


run MySQL/generate_mysql_json_normalized_workload.sh (scripts run for 5 minutes )

Next: watch PMM for workload

Note the workload is actually missing several indexes for the overall workload scripts, see if you can find them and optimize!
