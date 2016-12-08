// http://mongodb.github.io/node-mongodb-native/2.1/reference/ecmascript6/crud/
// http://mongodb.github.io/node-mongodb-native/2.1/api/index.html
import mongodb from 'mongodb';
import createDebug from 'debug';
import events from 'events';

const debug = createDebug('app:mongodb');
const logError = createDebug('app:mongodb:error');
logError.log = console.error.bind(console);
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
    event.emit('error', new Error('Mongodb connection error'));
  }
}

function getdb() {
  if (connectionInstance) {
    return connectionInstance;
  }
  return new Promise((resolve, reject) => {
    event.on('connect', () => {
      debug('connected on promise resolution to existing connectionInstance');
      resolve(connectionInstance);
    });
    event.on('error', (error) => {
      logError('mongodb.on(error): new connection with instance: %o', connectionInstance);
      reject(error);
    });
  });
}

const mongo = {
  connect,
  getdb,
};

export default mongo;
