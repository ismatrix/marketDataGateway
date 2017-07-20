import createDebug from 'debug';
import createDataFeed from './dataFeed';
import subscriber from './subscriber';
import { redis } from './redis';

// 管理datafeed在一个队列里面
const debug = createDebug('app:dataFeeds');
const logError = createDebug('app:dataFeeds:error');
logError.log = console.error.bind(console);

const dataFeedsArr = [];

const matchDataFeed = newConfig => elem => (
  elem.config.name === newConfig.name
);

function addPublisherListenerToDataFeed(dataFeed) {
  try {
    const liveDataTypeNames = dataFeed.getLiveDataTypeNames();
    debug('liveDataTypeNames %o', liveDataTypeNames);

    liveDataTypeNames.forEach((dataType) => {
      const listenerCount = dataFeed.listenerCount(dataType);
      debug('%o listener(s) of %o event', listenerCount, dataType);

      if (listenerCount === 0) {
        debug('adding listener on dataType %o of dataFeed %o', dataType, dataFeed.config.name);
        dataFeed
          .on(dataType, (data) => {
            try {
              const subID = dataFeed.mdToSubID(data);
              redis.multi()
                .publish(redis.join(redis.SUBID_MD, subID), JSON.stringify(data))
                .set(redis.join(redis.SUBID_LASTMD, subID), JSON.stringify(data))
                .execAsync()
                ;
            } catch (error) {
              logError('dataFeed.on(dataType): %o', error);
            }
          })
          .on('error', error => logError('dataFeed.on(error): %o', error))
          ;
      }
    });
  } catch (error) {
    logError('addPublisherListenerToDataFeed(): %o', error);
    throw error;
  }
}

function addConnectListenerToDataFeed(dataFeed) {
  try {
    const CONNECT_EVENT_NAME = 'connect:success';
    const listenerCount = dataFeed.listenerCount(CONNECT_EVENT_NAME);
    debug('%o listener(s) of %o event', listenerCount, CONNECT_EVENT_NAME);

    if (listenerCount === 0) {
      debug('adding listener on event %o of dataFeed %o', CONNECT_EVENT_NAME, dataFeed.config.name);
      dataFeed
        .on(CONNECT_EVENT_NAME, async () => {
          try {
            const globalSubIDs = await redis.smembersAsync(
              redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED));
            debug('globalSubIDs %o', globalSubIDs);
            globalSubIDs
              .filter(subID => redis.getKeyParts(redis.SUBID, subID, 'dataFeedName')[0] === dataFeed.config.name)
              .forEach(async (newSubID) => {
                try {
                  const sub = subscriber.subIDToSub(newSubID);
                  await dataFeed.subscribe(sub);
                  debug('subscribed to %o on dataFeed reconnect', sub);
                } catch (error) {
                  logError('needSubscribeSubIDs.forEach(): %o', error);
                }
              });
          } catch (error) {
            logError('dataFeed.on(%o): %o', CONNECT_EVENT_NAME, error);
          }
        })
        .on('error', error => logError('dataFeed.on(error): %o', error))
        ;
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

    if (newDataFeed.getLiveDataTypeNames().length) {
      // register only live data providers
      addPublisherListenerToDataFeed(newDataFeed);
      addConnectListenerToDataFeed(newDataFeed);
    }

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
