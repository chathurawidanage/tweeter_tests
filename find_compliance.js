let Mongo = require("./mongo_utils");

Mongo.getClient().then(client => {
    let db = client.db("Tweets");
    Mongo.createCollection(db, "compliance").then(compliance => {
        Mongo.createCollection(db, "tweets").then(async tweets => {
            let all = compliance.find();
            let t1 = Date.now();
            await all.forEach(record => {
                if (record.delete && record.delete.favorite && record.delete.favorite.tweet_id_str) {
                    let tweets_for_id = tweets.find({id_str: {$eq: record.delete.favorite.tweet_id_str}});
                    tweets_for_id.forEach(tweet => {
                        console.log(tweet.id_str,avorite.tweet_id_str);
                        // found
                    });
                } else {
                    // other records
                }
            });
            console.log("Time taken ", Date.now() - t1);
        });
    });
});