import createDebug from 'debug';
import { remove } from 'lodash';

const matchSubscription = newSub => sub => (
  sub.symbol === newSub.symbol &&
  sub.resolution === newSub.resolution &&
  sub.dataType === newSub.dataType);

const debug = createDebug('app:subscriptionStore');
const logError = createDebug('app:subscriptionStore:error');
logError.log = console.error.bind(console);

export default function createSubscriptionStore(config) {
  try {
    const subsArrCollection = {};

    const addSub = (newSub, collectionName) => {
      try {
        if (!Array.isArray(subsArrCollection[collectionName])) {
          subsArrCollection[collectionName] = [];
        }
        const similarSubIndex = subsArrCollection[collectionName]
          .findIndex(matchSubscription(newSub));

        if (similarSubIndex === -1) {
          subsArrCollection[collectionName].push(newSub);
          debug('added new sub %o', `${newSub.symbol}:${newSub.dataType}`);
        } else {
          debug('sub already in store %o', `${newSub.symbol}:${newSub.dataType}`);
        }
      } catch (error) {
        logError('addSub(): %o', error);
        throw error;
      }
    };

    const getSubs = (collectionName) => {
      try {
        if (collectionName === undefined) return subsArrCollection;
        if (!Array.isArray(subsArrCollection[collectionName])) return [];

        return subsArrCollection[collectionName];
      } catch (error) {
        logError('getSubs(): %o', error);
        throw error;
      }
    };

    const getDataFeedsSubscriptions = () => {
      try {
        const dataFeedsNames = Object.getOwnPropertyNames(subsArrCollection);
        const dataFeedsSubscriptions = dataFeedsNames.map(elem => ({
          dataFeedName: elem,
          subscriptions: subsArrCollection[elem],
        }));

        return dataFeedsSubscriptions;
      } catch (error) {
        logError('getDataFeedsSubscriptions(): %o', error);
        throw error;
      }
    };

    const isSubscribed = (mdData, collectionName) => {
      try {
        if (!Array.isArray(subsArrCollection[collectionName])) {
          return false;
        }

        const similarSubIndex = subsArrCollection[collectionName]
          .findIndex(matchSubscription(mdData));

        if (similarSubIndex !== -1) {
          return true;
        }
        return false;
      } catch (error) {
        logError('isSubscribed(): %o', error);
        throw error;
      }
    };

    const removeSub = (subToRemove, collectionName) => {
      try {
        if (!Array.isArray(subsArrCollection[collectionName])) return;

        const similarSubIndex = subsArrCollection[collectionName]
          .findIndex(matchSubscription(subToRemove));

        if (similarSubIndex !== -1) {
          const removedSub = subsArrCollection[collectionName]
            .splice(similarSubIndex, 1);
          debug('removeSub %o', removedSub);
        }
      } catch (error) {
        logError('removeSub(): %o', error);
        throw error;
      }
    };

    const removeDataTypeSubs = (dataType, collectionName) => {
      try {
        if (!Array.isArray(subsArrCollection[collectionName])) return;

        const removedDataTypeSubs = remove(subsArrCollection[collectionName],
          storeSub => storeSub.dataType === dataType);
        debug('removeDataTypeSubs %o', removedDataTypeSubs);
      } catch (error) {
        logError('removeDataTypeSubs(): %o', error);
        throw error;
      }
    };

    const subscriptionsStoreBase = {
      config,
      addSub,
      getSubs,
      getDataFeedsSubscriptions,
      isSubscribed,
      removeSub,
      removeDataTypeSubs,
    };

    return subscriptionsStoreBase;
  } catch (error) {
    logError('createSubscriptionStore(): %o', error);
    throw error;
  }
}
