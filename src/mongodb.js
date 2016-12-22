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

event.on('error', error => logError('event.on(error): %o', error));

async function connect(url) {
  try {
    connectionInstance = await MongoClient.connect(url, {
      reconnectTries: Number.MAX_VALUE,
      reconnectInterval: 1000,
      db: { bufferMaxEntries: 0 },
    });
    event.emit('connect');
  } catch (error) {
    debug('connect(): %o', error);
    event.emit('error', new Error('Mongodb connection error'));
    throw error;
  }
}

function getdb() {
  return new Promise((resolve, reject) => {
    if (connectionInstance) {
      resolve(connectionInstance);
    }
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
