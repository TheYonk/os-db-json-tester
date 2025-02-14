# os-db-json-tester
Scripts to test out JSON functionality in MySQL, PostgreSQL, and MongoDB... and simulate generate load


## macos setup

Setup mysql python module on mac
`brew install mysql pkg-config`

Install rest of dependencies
`pip install -r requirement.txt`

## Kick off the dataload test
`python bench/app_controller.py -f ./bench/app_config/xxx.json`