#!/bin/sh

run_time=1330

python3 mysql_normalized_lookups.py "$run_time" > logs/mysql_lu1.log 2>&1 &
python3 mysql_normalized_lookups.py "$run_time" > logs/mysql_lu2.log 2>&1 &

python3 mysql_json_normalized_quick_lookups.py "$run_time" > logs/mysql_qlu1.log 2>&1 &
python3 mysql_json_normalized_quick_lookups.py "$run_time" > logs/mysql_qlu2.log 2>&1 &
python3 mysql_json_normalized_quick_lookups.py "$run_time" > logs/mysql_qlu3.log 2>&1 &
python3 mysql_json_normalized_quick_lookups.py "$run_time" > logs/mysql_qlu4.log 2>&1 &

python3 mysql_json_normalized_aggregates.py "$run_time" > logs/mysql_agg1.log 2>&1 &
python3 mysql_json_normalized_aggregates.py "$run_time" > logs/mysql_agg2.log 2>&1 &

python3 mysql_json_normalized_batch_updates.py "$run_time" > logs/mysql_batch_1.log 2>&1 &

python3 mysql_json_normalized_insert_update_comments.py "$run_time" > logs/mysql_lu1.log 2>&1 &
python3 mysql_json_normalized_insert_update_comments.py "$run_time" > logs/mysql_lu2.log 2>&1 &
python3 mysql_json_normalized_insert_update_comments.py "$run_time" > logs/mysql_lu3.log 2>&1 &
python3 mysql_json_normalized_insert_update_comments.py "$run_time" > logs/mysql_lu4.log 2>&1 &


