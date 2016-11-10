import createDebug from 'debug';
import { differenceWith, isEqual } from 'lodash';
import createDataFeed from './dataFeed';
import subStores from './subscriptionStores';
import mdStores from './marketDataStores';

const debug = createDebug('dataFeeds');

const dataFeedsArr = [];

const matchDataFeed = newConfig => elem => (
  elem.config.name === newConfig.name
);

async function addDataFeed(config) {
  try {
    debug('addDataFeed() config %o', config);
    const existingDataFeed = dataFeedsArr.find(matchDataFeed(config));
    debug('existingDataFeed %o', existingDataFeed);
    if (existingDataFeed !== undefined) return;

    const newDataFeed = createDataFeed(config);

    mdStores.addAndGetMdStore({ dataFeed: newDataFeed });

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

const subscribe = async (theDataFeedName, newSub) => {
  try {
    const globalSubStore = subStores.addAndGetSubStore({ name: 'global' });

    const isGloballySubscribed = globalSubStore.isSubscribed(newSub, theDataFeedName);
    debug('isGloballySubscribed %o', isGloballySubscribed);

    if (isGloballySubscribed) return newSub;

    const theDataFeed = getDataFeed(theDataFeedName);
    await theDataFeed.subscribe(newSub);
    globalSubStore.addSub(newSub, theDataFeedName);
    return newSub;
  } catch (error) {
    debug('Error subscribe(): %o', error);
  }
};

const unsubscribe = async (theDataFeedName, subToRemove) => {
  try {
    const globalSubStore = subStores.addAndGetSubStore({ name: 'global' });

    const allSimilarSubs = subStores.countAllSub(subToRemove, theDataFeedName);

    if (allSimilarSubs > 1) return subToRemove;

    const theDataFeed = getDataFeed(theDataFeedName);
    await theDataFeed.unsubscribe(subToRemove);
    globalSubStore.removeSub(subToRemove, theDataFeedName);
    return subToRemove;
  } catch (error) {
    debug('Error unsubscribe(): %o', error);
  }
};

const clearGlobalSubsDiff = async () => {
  try {
    const theSubStores = subStores.getSubStores();
    const mergedGlobalSubStore = theSubStores.reduce((acc, cur) => {
      if (cur.config.name === 'global') return acc;
      const curStore = cur.getSubs();
      const keys = Object.getOwnPropertyNames(curStore);
      for (const key of keys) {
        // debug('%o of %o: %o', key, cur.config.name, curStore[key]);
        // debug('acc[%o]: %o', key, acc[key]);
        if (!Array.isArray(acc[key])) acc[key] = [];
        acc[key] = acc[key].concat(curStore[key]);
      }
      return acc;
    }, {});
    // debug('mergedGlobalSubStore %o', mergedGlobalSubStore);

    const globalSubStore = subStores.addAndGetSubStore({ name: 'global' });
    const globalSubs = globalSubStore.getSubs();
    // debug('globalSubs %o', globalSubs);

    const keys = Object.getOwnPropertyNames(globalSubs);
    for (const key of keys) {
      const mergedCollection = mergedGlobalSubStore[key];
      // debug('mergedCollection %o', mergedCollection);
      const globalCollection = globalSubs[key];
      // debug('globalCollection %o', globalCollection);
      const needUnsubscribe = differenceWith(globalCollection, mergedCollection, isEqual);
      debug('dataFeed %o needUnsubscribe %o', key, needUnsubscribe);
      const needSubscribe = differenceWith(mergedCollection, globalCollection, isEqual);
      debug('dataFeed %o needSubscribe %o', key, needSubscribe);

      needSubscribe.map(elem => subscribe(key, elem));
      needUnsubscribe.map(elem => unsubscribe(key, elem));
    }
  } catch (error) {
    debug('Error clearGlobalSubsDiff(): %o', error);
  }
};

const dataFeeds = {
  addDataFeed,
  getDataFeed,
  getSubscriptions,
  subscribe,
  unsubscribe,
  clearGlobalSubsDiff,
};

export default dataFeeds;
