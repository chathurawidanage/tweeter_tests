# this script can be used to load a json dump to mongodb
time mongoimport --db Tweets --collection compliance --file 2020-02-16-compliance.json --batchSize 1000