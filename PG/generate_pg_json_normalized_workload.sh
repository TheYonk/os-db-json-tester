#!/bin/sh

mkdir -p logs

run_time=330

python3 pg_json_normalized_lookups.py "$run_time" > logs/pg_lu1.log 2>&1 &
python3 pg_json_normalized_lookups.py "$run_time" > logs/pg_lu2.log 2>&1 &

python3 pg_json_normalized_quick_lookups.py "$run_time" > logs/pg_qlu1.log 2>&1 &
python3 pg_json_normalized_quick_lookups.py "$run_time" > logs/pg_qlu2.log 2>&1 &
python3 pg_json_normalized_quick_lookups.py "$run_time" > logs/pg_qlu3.log 2>&1 &
python3 pg_json_normalized_quick_lookups.py "$run_time" > logs/pg_qlu4.log 2>&1 &

python3 pg_json_normalized_aggregates.py "$run_time" > logs/pg_agg1.log 2>&1 &
python3 pg_json_normalized_aggregates.py "$run_time" > logs/pg_agg2.log 2>&1 &

python3 pg_json_normalized_batch_updates.py "$run_time" > logs/pg_batch_1.log 2>&1 &

python3 pg_json_normalized_insert_update_comments.py "$run_time" > logs/pg_lu1.log 2>&1 &
python3 pg_json_normalized_insert_update_comments.py "$run_time" > logs/pg_lu2.log 2>&1 &
python3 pg_json_normalized_insert_update_comments.py "$run_time" > logs/pg_lu3.log 2>&1 &
python3 pg_json_normalized_insert_update_comments.py "$run_time" > logs/pg_lu4.log 2>&1 &

