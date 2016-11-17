import createDebug from 'debug';
import { remove } from 'lodash';

const matchSubscription = newSub => sub => (
  sub.symbol === newSub.symbol &&
  sub.resolution === newSub.resolution &&
  sub.dataType === newSub.dataType);

export default function createSubscriptionStore(config) {
  const {
    name,
  } = config;
  const debug = createDebug(`${name}@subscriptionStore`);
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
        debug('Error addSub(): %o', error);
      }
    };

    const getSubs = (collectionName) => {
      try {
        if (collectionName === undefined) return subsArrCollection;
        if (!Array.isArray(subsArrCollection[collectionName])) return [];

        return subsArrCollection[collectionName];
      } catch (error) {
        debug('Error getSubs(): %o', error);
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
        debug('Error isSubscribed(): %o', error);
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
        debug('Error removeSub(): %o', error);
      }
    };

    const removeDataTypeSubs = (dataType, collectionName) => {
      try {
        if (!Array.isArray(subsArrCollection[collectionName])) return;

        const removedDataTypeSubs = remove(subsArrCollection[collectionName],
          storeSub => storeSub.dataType === dataType);
        debug('removeDataTypeSubs %o', removedDataTypeSubs);
      } catch (error) {
        debug('Error removeDataTypeSubs(): %o', error);
      }
    };

    const subscriptionsStoreBase = {
      config,
      addSub,
      getSubs,
      isSubscribed,
      removeSub,
      removeDataTypeSubs,
    };

    return subscriptionsStoreBase;
  } catch (error) {
    debug('Error createSubscriptionStore() %o', error);
  }
}
