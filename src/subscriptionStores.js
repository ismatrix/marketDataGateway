import createDebug from 'debug';
import { remove } from 'lodash';
import createSubscriptionStore from './subscriptionStore';

const debug = createDebug('subscriptionStores');

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
    debug('Error addAndGetSubStore(): %o', error);
  }
}

function getSubStores() {
  try {
    return subStores;
  } catch (error) {
    debug('Error addAndGetSubStore(): %o', error);
  }
}

function removeSubStore(config) {
  try {
    const removedSubStore = remove(subStores, matchSubStore(config));
    debug('removedSubStore %o', removedSubStore);
  } catch (error) {
    debug('Error removeSubStore(): %o', error);
  }
}

function getAllSubs(collectionName) {
  try {
    const allSubs = subStores.reduce((acc, cur) =>
      acc.concat(cur.getSubs(collectionName))
    , []);
    return allSubs;
  } catch (error) {
    debug('Error getAllSubs(): %o', error);
  }
}

function countAllSub(subToCount, collectionName) {
  try {
    const allSubs = getAllSubs(collectionName);
    const allSimilarSubs = allSubs.filter(matchSubscription(subToCount));
    debug('number of similar sub %o', allSimilarSubs.length);
    return allSimilarSubs.length;
  } catch (error) {
    debug('Error countAllSub(): %o', error);
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
    debug('Error getSubscriptionsStores(): %o', error);
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
