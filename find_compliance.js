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

            let all = compliance.find();
            let t1 = Date.now();
            all.forEach(record => {
                if (record.delete && record.delete.favorite && record.delete.favorite.tweet_id_str) {
                    find_tweet(record.delete.favorite.tweet_id_str);
                } else if (record.delete && record.delete.status && record.delete.status.id_str) {
                    find_tweet(record.delete.status.id_str);
                } else {
                    // other records
                    //console.log("record",record)
                }
            }).then(done => {
                console.log("Time taken ", Date.now() - t1);
            }).catch(err => {
                console.error("Error in forEach", err);
            });
        });
    });
});