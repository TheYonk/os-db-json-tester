# os-db-json-tester
Scripts to test out JSON functionality in MySQL, PostgreSQL, and MongoDB... and simulate generate load


## macos setup

Setup mysql python module on mac
`brew install mysql pkg-config`

Install rest of dependencies
`pip install -r requirement.txt`

## Kick off the dataload test
`python bench/app_controller.py -f ./bench/app_config/xxx.json`

## Containerize (Postgres only)

Packaging this as container makes it portable, this is really build once and run anywhere.  


### How to build it.

To build run the below command in the root of the repo

```
docker build -t <repo-name>/os-db-json-tester .
```

to push to a repo do the below
```
$docker push <repo-name>/os-db-json-tester

Using default tag: latest
The push refers to repository [docker.io/<repo-name>/os-db-json-tester]
9315c4821e72: Layer already exists 
ffcbb8009fdd: Layer already exists 
66bf13b6394e: Pushed 
18853e9f6f52: Pushed 
c29f5b76f736: Layer already exists 
7c2a2b22c1bc: Layer already exists 
77a065dccb04: Layer already exists 
4317291ed165: Pushed 
b26995e9f45f: Layer already exists 
7e8ac65e25aa: Layer already exists 
latest: digest: sha256:5288fc18f671c1e595dd81f11f214de35473758b48f44b5b8e2e1a8b0023f36a size: 856
```

### How to run it locally

This container depends on 4 envvar

```
"HOSTNAME" 
"USERNAME" 
"PASSWORD" 
"DATABASE" 
```

`start_bench.sh` script uses that to generate its config file dynamically. 

so change the `.envfile` accordingly
```
$cat .envfile 
HOSTNAME=host.docker.internal
USERNAME=admin
PASSWORD=password
DATABASE=public
```

now to start the container do this
```
$docker run -it --env-file .envfile dhilipkumars/os-db-json-tester 
Namespace(myfile='./config.json', nopmm=0, verbose=0, special=0, function='', threads=1, time=-1, myclient='connector')
INFO:root:Starting PG
inside load function
INFO:root:Loading Actors...
INFO:root:Loading Titles & ID's...
INFO:root:Loading Movies and Directors with Many Movies
sh: 1: pmm-admin: not found
INFO:root:Started General Website Workload as Pid 361
INFO:root:General Website Workload at count: 1
INFO:root:pid: 361
INFO:root:Active List: {361: 1}
INFO:root:Started General Website Workload as Pid 376
INFO:root:General Website Workload at count: 2
INFO:root:pid: 376
INFO:root:Active List: {361: 1, 376: 1}
INFO:root:Started General Website Workload as Pid 390
INFO:root:General Website Workload at count: 3
INFO:root:pid: 390
INFO:root:Active List: {361: 1, 376: 1, 390: 1}
INFO:root:Started General Website Workload as Pid 404
INFO:root:General Website Workload at count: 4
INFO:root:pid: 404
.
.
.
.

```
loose the `-it` from the command to run in the background.

## How to deploy in a kube cluster

If postgres is running in kube what are the odds its application will outside in a VM? :p , lets run it next to the db

The applies a deployment and configmap to the target kube cluster, the target kube should be already reachable from your `kubectl`

### Change the config

Change the envvar values in the below config file
```
$cat kustomization.yaml 
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - deployment.yaml

configMapGenerator:
  - name: db-env-config
    literals:
      - HOSTNAME=hostname
      - USERNAME=username
      - PASSWORD=password
      - DATABASE=dbname

patches:
  - path: toleration-patch.yaml
    target:
      kind: Deployment
      name: os-db-json-tester

```
for env's that dont need to tolerate nodes, simply remove it from the `kustomization.yaml`

### How to deploy

```
$ k -n ${Namespace} apply -k .
configmap/db-env-config-7dktg45g2c created
deployment.apps/os-db-json-tester created
```

you can check if its running
```
$ k -n ${Namespace} get pods -l app=os-db-json-tester
NAME                                READY   STATUS    RESTARTS   AGE
os-db-json-tester-99d98cb87-cl7d9   1/1     Running   0          2m43s
```

check its logs by 

```
$k -n ${Namespace} logs -f os-db-json-tester-99d98cb87-cl7d9
Namespace(myfile='./config.json', nopmm=0, verbose=0, special=0, function='', threads=1, time=-1, myclient='connector')
INFO:root:Starting PG
inside load function
INFO:root:Loading Actors...
INFO:root:Loading Titles & ID's...
INFO:root:Loading Movies and Directors with Many Movies
sh: 1: pmm-admin: not found
INFO:root:Started General Website Workload as Pid 361
INFO:root:General Website Workload at count: 1
INFO:root:pid: 361
.
.
.
.

```
stop it by

```
$k -n ${Namespace} delete -k .
configmap "db-env-config-7dktg45g2c" deleted
deployment.apps "os-db-json-tester" deleted
```
