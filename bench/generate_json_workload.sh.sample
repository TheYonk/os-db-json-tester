#!/bin/bash

NAME="MYSQL"
DESC=""
APP=1
HOST="localhost"
USER="movie_json_user"
PASS="Ch@nge_me_1st"
DB="movie_json_test"
ACT=1
TYPE="mysql"
BENCHTIME=1800
BENCHSCRIPT="FULL_TEST.sh"

rm -f  ${BENCHSCRIPT}
touch ${BENCHSCRIPT}
echo "#!/bin/bash" >> ${BENCHSCRIPT}
chmod 755 ${BENCHSCRIPT}

print_json()
{
	#echo "Test"
	#jq --null-input --arg name "${NAME}" --arg desc "${DESC}" --arg appname "${APP_NODE}" '{"name" : $name, desc : $desc}
	X="
    {
    \"name\": \"${NAME}\",
    \"desc\": \"${DESC}\",
    \"appnode\": ${APP},
    \"host\": \"${HOST}\",
    \"username\": \"${USER}\",
    \"password\": \"${PASS}\",
    \"database\": \"${DB}\",
    \"bench_active\": 1,
    \"type\": \"${TYPE}\",
    \"website_workload\": ${WW},
    \"reporting_workload\": ${RW},
    \"comments_workload\": ${CW},
    \"longtrans_workload\": ${LTW},
    \"read_only_workload\": ${ROW},
    \"list_workload\": ${LW},
    \"special_workload\": ${SW}
    }
    "
    echo $X > ./app_config/${TYPE}_${APP}_${NAME}.json
    write_bench
}

write_bench()
{
	echo -e "python app_controller.py -f ./app_config/${TYPE}_${APP}_${NAME}.json -t ${BENCHTIME}" >> ${BENCHSCRIPT}
	echo -e "sleep 60" >> ${BENCHSCRIPT}
}

NAME="read_only"
WW=0
RW=0
CW=0
LTW=0
ROW=10
LW=0
SW=0

print_json 

NAME="kitchen"
WW=2
RW=2
CW=10
LTW=0
ROW=5
LW=3
SW=2

print_json 

NAME="med_web"
WW=20
RW=0
CW=0
LTW=0
ROW=0
LW=0
SW=0

print_json 

NAME="lrg_web"
WW=50
RW=0
CW=0
LTW=0
ROW=0
LW=0
SW=0

print_json 
	
NAME="sm_write"
WW=0
RW=0
CW=10
LTW=0
ROW=0
LW=0
SW=0

print_json 
	
NAME="lrg_write"
WW=0
RW=0
CW=25
LTW=0
ROW=0
LW=0
SW=0

print_json 
	
NAME="5050_med"
WW=10
RW=0
CW=10
LTW=0
ROW=0
LW=0
SW=0

print_json 

NAME="5050_large"
WW=20
RW=0
CW=20
LTW=0
ROW=0
LW=0
SW=0

print_json 
	
NAME="base_sm"
WW=5
RW=2
CW=5
LTW=0
ROW=0
LW=0
SW=0

print_json 
	
NAME="base_med"
WW=10
RW=4
CW=10
LTW=0
ROW=0
LW=0
SW=0

print_json 

NAME="base_lrg"
WW=15
RW=6
CW=15
LTW=0
ROW=0
LW=0
SW=0

print_json 

NAME="complete_web"
WW=10
RW=0
CW=10
LTW=0
ROW=0
LW=2
SW=1

print_json 

NAME="report"
WW=0
RW=5
CW=0
LTW=0
ROW=0
LW=0
SW=0

print_json 

NAME="multirow"
WW=0
RW=0
CW=0
LTW=0
ROW=0
LW=5
SW=0

print_json 

NAME="multirow"
WW=0
RW=0
CW=0
LTW=0
ROW=0
LW=0
SW=5

print_json 