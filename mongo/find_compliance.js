let Mongo = require("./mongo_utils");

Mongo.getClient().then(client => {
    let db = client.db("Tweets");
    Mongo.createCollection(db, "compliance").then(compliance => {
        Mongo.createCollection(db, "tweets").then(async tweets => {

            function find_tweet(id_str) {
                let tweets_for_id = tweets.find({id_str: {$eq: id_str}});
                tweets_for_id.forEach(tweet => {
                    console.log(tweet.id_str, id_str);
                    // found
                });
            }

            let all = compliance.find({"delete.status":{$exists:true}});
            let t1 = Date.now();
            all.forEach(record => {
                find_tweet(record.delete.status.id_str);
            }).then(done => {
                console.log("Time taken ", Date.now() - t1);
            }).catch(err => {
                console.error("Error in forEach", err);
            });
        });
    });
});