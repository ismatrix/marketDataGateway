// http://mongodb.github.io/node-mongodb-native/2.2/reference/ecmascript6/crud/
// http://mongodb.github.io/node-mongodb-native/2.2/api/index.html
import mongodb from 'mongodb';
import createDebug from 'debug';
import events from 'events';

const debug = createDebug('mongodb');
const event = new events.EventEmitter();
const MongoClient = mongodb.MongoClient;

let connectionInstance;
let gurl;

async function connect(url) {
  gurl = url;
  try {
    connectionInstance = await MongoClient.connect(gurl);
    event.emit('connect');
  } catch (err) {
    debug('Mongodb connect Err: %s', err);
    event.emit('error');
  }
}

function getdb() {
  if (connectionInstance) {
    // debug('existing connection');
    return connectionInstance;
  }
  return new Promise((resolve, reject) => {
    event.on('connect', () => {
      debug('connected on promise resolution to existing connectionInstance');
      resolve(connectionInstance);
    });
    event.on('error', () => {
      debug('new connection with instance: %o', connectionInstance);
      reject(new Error('Error connection'));
    });
  });
}

const mongo = {
  connect,
  getdb,
};

export default mongo;
