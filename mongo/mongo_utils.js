const MongoClient = require('mongodb').MongoClient;
const f = require('util').format;
const url = 'mongodb://%s:%s@localhost:27017?authMechanism=%s';

module.exports.createCollection = async function createCollection(db, collection) {
    const collections = await db.collections();
    if (!collections.map(c => c.s.name).includes(collection)) {
        console.log("Creating new collection", collection);
        await db.createCollection(collection);
    }
    return db.collection(collection);
};


let connectedClient = null;

const user = encodeURIComponent(process.env.MNG_USER);
const password = encodeURIComponent(process.env.MNG_PW);
const authMechanism = 'DEFAULT';

const options = {keepAlive: 300000, connectTimeoutMS: 30000};

module.exports.getClient = function getClient() {
    if (connectedClient != null) {
        return connectedClient;
    } else {
        connectedClient = MongoClient.connect(f(url, user, password, authMechanism), options);
        return connectedClient;
    }
};