import createDebug from 'debug';
import bluebird from 'bluebird';
import createRedis from 'redis';
import createDataFeed from './dataFeed';

const debug = createDebug('app:dataFeeds');
const logError = createDebug('app:dataFeeds:error');
logError.log = console.error.bind(console);
bluebird.promisifyAll(createRedis.RedisClient.prototype);
bluebird.promisifyAll(createRedis.Multi.prototype);
const redis = createRedis.createClient({ port: 6379 });

const dataFeedsArr = [];
const MD_ROOM = 'md';
const LAST_MD = 'subid.lastMd';

const matchDataFeed = newConfig => elem => (
  elem.config.name === newConfig.name
);

function addPublisherListenerToDataFeed(dataFeed) {
  try {
    const liveDataTypeNames = dataFeed.getLiveDataTypeNames();
    debug('liveDataTypeNames %o', liveDataTypeNames);

    for (const dataType of liveDataTypeNames) {
      const listenerCount = dataFeed.listenerCount(dataType);
      debug('%o listener(s) of %o event', listenerCount, dataType);

      if (listenerCount === 0) {
        debug('adding listener on dataType %o of dataFeed %o', dataType, dataFeed.config.name);
        dataFeed
          .on(dataType, (data) => {
            try {
              const subID = dataFeed.mdToSubID(data);
              redis.multi()
                .publish([MD_ROOM, subID].join(':'), JSON.stringify(data))
                .set([LAST_MD, subID].join(':'), JSON.stringify(data))
                .execAsync()
                ;
            } catch (error) {
              logError('dataFeed.on(dataType): %o', error);
            }
          })
          .on('error', error => logError('dataFeed.on(error): %o', error))
          ;
      }
    }
  } catch (error) {
    logError('addPublisherListenerToDataFeed(): %o', error);
    throw error;
  }
}

async function addDataFeed(config) {
  try {
    debug('addDataFeed() config %o', config);
    const existingDataFeed = dataFeedsArr.find(matchDataFeed(config));
    debug('existingDataFeed %o', existingDataFeed);
    if (existingDataFeed !== undefined) return;

    const newDataFeed = createDataFeed(config);
    if ('init' in newDataFeed) await newDataFeed.init();
    if ('connect' in newDataFeed) await newDataFeed.connect();

    addPublisherListenerToDataFeed(newDataFeed);

    dataFeedsArr.push(newDataFeed);
  } catch (error) {
    logError('addDataFeed(): %o', error);
    throw error;
  }
}

function getDataFeed(dataFeedName) {
  try {
    const theDataFeed = dataFeedsArr.find(elem => elem.config.name === dataFeedName);
    if (theDataFeed !== undefined) return theDataFeed;

    throw new Error('dataFeed not found');
  } catch (error) {
    logError('getDataFeed(): %o', error);
    throw error;
  }
}

function getDataFeedsByNames(dataFeedNames) {
  try {
    const theDataFeeds = dataFeedsArr.filter(df => dataFeedNames.includes(df.config.name));
    return theDataFeeds;
  } catch (error) {
    logError('getDataFeedsByNames(): %o', error);
    throw error;
  }
}

const dataFeeds = {
  addDataFeed,
  getDataFeed,
  getDataFeedsByNames,
};

export default dataFeeds;
