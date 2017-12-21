import logger from 'sw-common';
import createDataFeed from './dataFeed';
import subscriber from './subscriber';
import { redis } from './redis';

// 管理datafeed在一个队列里面
const dataFeedsArr = [];

const matchDataFeed = newConfig => elem => (
  elem.config.name === newConfig.name
);

function addPublisherListenerToDataFeed(dataFeed) {
  try {
    const liveDataTypeNames = dataFeed.getLiveDataTypeNames();
    logger.debug('liveDataTypeNames %j', liveDataTypeNames);

    liveDataTypeNames.forEach((dataType) => {
      const listenerCount = dataFeed.listenerCount(dataType);
      logger.debug('%j listener(s) of %j event', listenerCount, dataType);

      if (listenerCount === 0) {
        logger.debug('adding listener on dataType %j of dataFeed %j', dataType, dataFeed.config.name);
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
              logger.error('dataFeed.on(dataType): %j', error);
            }
          })
          .on('error', error => logger.error('dataFeed.on(error): %j', error))
          ;
      }
    });
  } catch (error) {
    logger.error('addPublisherListenerToDataFeed(): %j', error);
    throw error;
  }
}

function addConnectListenerToDataFeed(dataFeed) {
  try {
    const CONNECT_EVENT_NAME = 'connect:success';
    const listenerCount = dataFeed.listenerCount(CONNECT_EVENT_NAME);
    logger.debug('%j listener(s) of %j event', listenerCount, CONNECT_EVENT_NAME);

    if (listenerCount === 0) {
      logger.debug('adding listener on event %j of dataFeed %j', CONNECT_EVENT_NAME, dataFeed.config.name);
      dataFeed
        .on(CONNECT_EVENT_NAME, async () => {
          try {
            const globalSubIDs = await redis.smembersAsync(
              redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED));
            logger.debug('globalSubIDs %j', globalSubIDs);
            globalSubIDs
              .filter(subID => redis.getKeyParts(redis.SUBID, subID, 'dataFeedName')[0] === dataFeed.config.name)
              .forEach(async (newSubID) => {
                try {
                  const sub = subscriber.subIDToSub(newSubID);
                  await dataFeed.subscribe(sub);
                  logger.debug('subscribed to %j on dataFeed reconnect', sub);
                } catch (error) {
                  logger.error('needSubscribeSubIDs.forEach(): %j', error);
                }
              });
          } catch (error) {
            logger.error('dataFeed.on(%j): %j', CONNECT_EVENT_NAME, error);
          }
        })
        .on('error', error => logger.error('dataFeed.on(error): %j', error))
        ;
    }
  } catch (error) {
    logger.error('addPublisherListenerToDataFeed(): %j', error);
    throw error;
  }
}

async function addDataFeed(config) {
  try {
    logger.debug('addDataFeed() config %j', config);
    const existingDataFeed = dataFeedsArr.find(matchDataFeed(config));
    logger.debug('existingDataFeed %j', existingDataFeed);
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
    logger.error('addDataFeed(): %j', error);
    throw error;
  }
}

function getDataFeed(dataFeedName) {
  try {
    const theDataFeed = dataFeedsArr.find(elem => elem.config.name === dataFeedName);
    if (theDataFeed !== undefined) return theDataFeed;

    throw new Error('dataFeed not found');
  } catch (error) {
    logger.error('getDataFeed(): %j', error);
    throw error;
  }
}

function getDataFeedsByNames(dataFeedNames) {
  try {
    const theDataFeeds = dataFeedsArr.filter(df => dataFeedNames.includes(df.config.name));
    return theDataFeeds;
  } catch (error) {
    logger.error('getDataFeedsByNames(): %j', error);
    throw error;
  }
}

const dataFeeds = {
  addDataFeed,
  getDataFeed,
  getDataFeedsByNames,
};

export default dataFeeds;
