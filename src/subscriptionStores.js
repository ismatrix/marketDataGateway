import createDebug from 'debug';
import { remove } from 'lodash';
import createSubscriptionStore from './subscriptionStore';

const debug = createDebug('subscriptionStores');
const logError = createDebug('app:subscriptionStores:error');
logError.log = console.error.bind(console);

const subStores = [];

const matchSubStore = newConfig => elem => (
  elem.config.name === newConfig.name
);
const matchSubscription = newSub => elem => (
  elem.dataFeedName === newSub.dataFeedName &&
  elem.symbol === newSub.symbol &&
  elem.resolution === newSub.resolution &&
  elem.dataType === newSub.dataType);


function addAndGetSubStore(config) {
  try {
    const existingSubStore = subStores.find(matchSubStore(config));

    if (existingSubStore !== undefined) return existingSubStore;

    const newSubStore = createSubscriptionStore(config);
    subStores.push(newSubStore);
    return newSubStore;
  } catch (error) {
    logError('addAndGetSubStore(): %o', error);
    throw error;
  }
}

function getSubStores() {
  try {
    return subStores;
  } catch (error) {
    logError('addAndGetSubStore(): %o', error);
    throw error;
  }
}

function removeSubStore(config) {
  try {
    const removedSubStore = remove(subStores, matchSubStore(config));
    debug('removedSubStore %o', removedSubStore);
  } catch (error) {
    logError('removeSubStore(): %o', error);
    throw error;
  }
}

function getAllSubs(collectionName) {
  try {
    const allSubs = subStores.reduce((acc, cur) =>
      acc.concat(cur.getSubs(collectionName))
    , []);
    return allSubs;
  } catch (error) {
    logError('getAllSubs(): %o', error);
    throw error;
  }
}

function countAllSub(subToCount, collectionName) {
  try {
    const allSubs = getAllSubs(collectionName);
    const allSimilarSubs = allSubs.filter(matchSubscription(subToCount));
    debug('number of similar sub %o', allSimilarSubs.length);
    return allSimilarSubs.length;
  } catch (error) {
    logError('countAllSub(): %o', error);
    throw error;
  }
}

function getSubscriptionsStores() {
  try {
    const subscriptionsStores = subStores.map(elem => ({
      storeName: elem.config.name,
      dataFeedsSubscriptions: elem.getDataFeedsSubscriptions(),
    }));

    return subscriptionsStores;
  } catch (error) {
    logError('getSubscriptionsStores(): %o', error);
    throw error;
  }
}

const subscriptionStoresBase = {
  addAndGetSubStore,
  removeSubStore,
  getSubStores,
  getAllSubs,
  countAllSub,
  getSubscriptionsStores,
};

export default subscriptionStoresBase;
