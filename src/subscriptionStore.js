import createDebug from 'debug';
import { remove } from 'lodash';

const matchSubscription = newSub => sub => (
  sub.dataFeedName === newSub.dataFeedName &&
  sub.symbol === newSub.symbol &&
  sub.resolution === newSub.resolution &&
  sub.dataType === newSub.dataType);

export default function createSubscriptionStore(config) {
  const {
    name,
  } = config;
  const debug = createDebug(`${name}@subscriptionStore`);
  try {
    const subscriptionsArr = [];

    const addSub = (newSub) => {
      try {
        const similarSubIndex = subscriptionsArr.findIndex(matchSubscription(newSub));

        if (similarSubIndex === -1) {
          subscriptionsArr.push(newSub);
        }
      } catch (error) {
        debug('Error addSub(): %o', error);
      }
    };

    const getSubs = () => {
      try {
        return subscriptionsArr;
      } catch (error) {
        debug('Error getSubs(): %o', error);
      }
    };

    const isSubscribed = (mdData) => {
      try {
        const similarSubIndex = subscriptionsArr
          .findIndex(matchSubscription(mdData));

        if (similarSubIndex !== -1) return true;
        return false;
      } catch (error) {
        debug('Error isSubscribed(): %o', error);
      }
    };

    const removeSub = (subToRemove) => {
      try {
        const similarSubIndex = subscriptionsArr
          .findIndex(matchSubscription(subToRemove));

        if (similarSubIndex !== -1) {
          const removedSub = subscriptionsArr.splice(similarSubIndex, similarSubIndex + 1);
          debug('removeSub %o', removedSub);
        }
      } catch (error) {
        debug('Error removeSub(): %o', error);
      }
    };

    const removeDataTypeSubs = (dataType) => {
      try {
        const removedDataTypeSubs = remove(subscriptionsArr,
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
