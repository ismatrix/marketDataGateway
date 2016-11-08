import createDebug from 'debug';
import createDataFeed from './dataFeed';
import subStores from './subscriptionStores';

const debug = createDebug('dataFeeds');

const dataFeedsArr = [];

const marketDataStore = [];

const addMarketData = (data) => {
  try {
    const index = marketDataStore.findIndex(elem =>
      (
        elem.symbol === data.symbol &&
        elem.resolution === data.resolution &&
        elem.dataType === data.dataType
      )
    );
    if (index === -1) {
      marketDataStore.push(data);
    } else {
      marketDataStore[index] = data;
    }
  } catch (error) {
    debug('Error addMarketData(): %o', error);
  }
};

async function addDataFeed(config) {
  try {
    debug('addDataFeed() config %o', config);
    if (dataFeedsArr.map(elem => elem.config.name).includes(config.name)) return;

    const newDataFeed = createDataFeed(config);

    newDataFeed.config = config;

    for (const dataType of config.dataTypes) {
      newDataFeed
        .on(dataType, data => addMarketData(data))
        .on('error', error => debug('Error newDataFeed.onDataType: %o', error))
        ;
    }

    dataFeedsArr.push(newDataFeed);
  } catch (error) {
    debug('Error addDataFeed(): %o', error);
  }
}

function getDataFeed(dataFeedName) {
  try {
    const theDataFeed = dataFeedsArr.find(elem => elem.config.name === dataFeedName);
    if (theDataFeed !== undefined) return theDataFeed;

    throw new Error('dataFeed not found');
  } catch (error) {
    debug('Error getDataFeed(): %o', error);
  }
}

const getSubscriptions = () => {
  try {
    const globalSubStore = subStores.addAndGetSubStore({ name: 'global' });
    return globalSubStore;
  } catch (error) {
    debug('Error getSubscriptions(): %o', error);
  }
};

const getMarketData = () => {
  try {
    return marketDataStore;
  } catch (error) {
    debug('Error getMarketData(): %o', error);
  }
};

const subscribe = async (theDataFeed, newSub) => {
  try {
    const theDataFeedSubs = subStores.addAndGetSubStore({ name: theDataFeed.config.name });

    const isSubscribed = theDataFeedSubs.isSubscribed(newSub);

    if (isSubscribed) return newSub;

    await theDataFeed.subscribe(newSub);
    theDataFeedSubs.addSub(newSub);
    return newSub;
  } catch (error) {
    debug('Error subscribe(): %o', error);
  }
};

const unsubscribe = async (theDataFeed, subToRemove) => {
  try {
    const theDataFeedSubs = subStores.addAndGetSubStore({ name: theDataFeed.config.name });

    const allSimilarSubs = subStores.countAllSub(subToRemove);

    if (allSimilarSubs > 1) return subToRemove;

    await theDataFeed.unsubscribe(subToRemove);
    theDataFeedSubs.removeSub(subToRemove);
    return subToRemove;
  } catch (error) {
    debug('Error unsubscribe(): %o', error);
  }
};

const dataFeeds = {
  addDataFeed,
  getDataFeed,
  getSubscriptions,
  getMarketData,
  subscribe,
  unsubscribe,
};

export default dataFeeds;
