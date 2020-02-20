const Twitter = require('twitter');
const Mongo = require("./mongo_utils");

const twitterClient = new Twitter({
    consumer_key: process.env.TWT_CON_KEY,
    consumer_secret: process.env.TWT_CON_SEC,
    access_token_key: process.env.TWT_AC_TOKE,
    access_token_secret: process.env.TWT_TOKE_SEC
});

Mongo.getClient().then(client => {
    let db = client.db("Tweets");
    Mongo.createCollection(db, 'tweets').then(tweetsCol => {
        console.log("Starting tweets stream...");
        const stream = twitterClient.stream('statuses/sample');
        let buffer = [];
        stream.on('data', function (event) {
            buffer.push(event);
            if (buffer.length > 1000) {
                const buffer_copy = buffer;
                buffer = [];
                tweetsCol.insertMany(buffer_copy).then(r => {
                    //console.debug(r);
                    // buffer copy is garbage collectible
                }).catch(err => {
                    console.log("Error in insertion", err);
                });
            }
        });
    });
});
