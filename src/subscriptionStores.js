import createDebug from 'debug';
import { remove } from 'lodash';
import createSubscriptionStore from './subscriptionStore';

const debug = createDebug('sessionSubscriptions.grpc');

const subStores = [];

const matchSubStore = newConfig => subStore => (
  subStore.config.name === newConfig.name
);
const matchSubscription = newSub => sub => (
  sub.dataFeedName === newSub.dataFeedName &&
  sub.symbol === newSub.symbol &&
  sub.resolution === newSub.resolution &&
  sub.dataType === newSub.dataType);


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

function removeSubStore(config) {
  try {
    const removedSubStore = remove(subStores, matchSubStore(config));
    debug('removedSubStore %o', removedSubStore);
  } catch (error) {
    debug('Error removeSubStore(): %o', error);
  }
}

function getAllSubs() {
  try {
    const allSubs = subStores.reduce((acc, cur) =>
      acc.concat(cur.getSubs())
    , []);
    return allSubs;
  } catch (error) {
    debug('Error getAllSubs(): %o', error);
  }
}

function countAllSub(subToCount) {
  try {
    const allSubs = getAllSubs();
    const allSimilarSubs = allSubs.filter(matchSubscription(subToCount));
    debug('number of similar sub %o', allSimilarSubs.length);
    return allSimilarSubs.length;
  } catch (error) {
    debug('Error countAllSub(): %o', error);
  }
}

const subscriptionStoresBase = {
  addAndGetSubStore,
  removeSubStore,
  getAllSubs,
  countAllSub,
};

export default subscriptionStoresBase;
