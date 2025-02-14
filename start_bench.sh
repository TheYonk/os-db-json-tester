#!/bin/sh


#set some defaults
# Set default values if environment variables are not set
HOSTNAME=${HOSTNAME:-"localhost"}
USERNAME=${USERNAME:-"postgres"}
PASSWORD=${PASSWORD:-"password"}
DATABASE=${DATABASE:-"public"}

# Extract the evnrionment variables and generate the config file
jq -n \
  --arg host "$HOSTNAME" \
  --arg user "$USERNAME" \
  --arg pass "$PASSWORD" \
  --arg db "$DATABASE" \
  '{
    name: "pg",
    desc: "postgresql Movie Database Test for: pg",
    appnode: 1,
    host: $host,
    username: $user,
    password: $pass,
    database: $db,
    bench_active: 1,
    type: "postgresql",
    website_workload: 18,
    reporting_workload: 3,
    comments_workload: 6,
    longtrans_workload: 0,
    title_idx: 1,
    year_idx: 1
  }' > ./config.json

# Run the benchmarking
python bench/app_controller.py -f ./config.json
