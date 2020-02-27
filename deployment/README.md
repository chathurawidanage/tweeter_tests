# Mongo deplyment 

This program is designed to deploy mongo on a number of machines.

```
mongo deploy --config FILE
mongo deploy --ips=10.0.0.[1-5] 
             --names=master,worker[2-5] 
             --shards=3
             --replicas=2
```

## Install

```
git clone https://github.iu.edu/truthy-team/MoeBackendReplacement.git
cd MoeBackendReplacement/development/cloudmesh-mongo
python3.8 -m venv MONGO
source MONGO/bin/activate
pip install -e .