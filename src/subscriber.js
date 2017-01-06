import createDebug from 'debug';
import { difference } from 'lodash';
import { redis, redisSub } from './redis';

const debug = createDebug('app:subscriber');
const logError = createDebug('app:subscriber:error');
logError.log = console.error.bind(console);

function subToSubID(dataFeedName, sub) {
  const subID = redis.joinSubKeys(dataFeedName, sub.dataType, sub.resolution, sub.symbol);
  return subID;
}

function subIDToSub(subID) {
  const [dataType, resolution, symbol] = redis.getKeyParts(redis.SUBID, subID, 'dataType', 'resolution', 'symbol');
  const subscription = {
    dataType,
    resolution,
    symbol,
  };
  return subscription;
}

async function subscribeDataFeed(sub, dataFeed) {
  try {
    const subID = subToSubID(dataFeed.config.name, sub);

    const isSubIDInGlobal = await redis.sismemberAsync(
      redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED), subID);

    if (!isSubIDInGlobal) {
      await dataFeed.subscribe(sub);

      await redis.saddAsync(
        redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED), subID);
    }

    return sub;
  } catch (error) {
    logError('subscribeDataFeed(): %o', error);
    throw error;
  }
}

async function unsubscribeDataFeed(sub, dataFeed) {
  try {
    debug('to be completed %o %o', sub, dataFeed);

    return sub;
  } catch (error) {
    logError('unsubscribeDataFeed(): %o', error);
    throw error;
  }
}

async function subscribeRedis(subID) {
  try {
    const subscribeRedisResult = await redisSub.subscribeAsync(redis.join(redis.SUBID_MD, subID));
    debug('subscribeRedisResult %o', subscribeRedisResult);

    return subscribeRedisResult;
  } catch (error) {
    logError('subscribeRedis(): %o', error);
    throw error;
  }
}

async function subscribeStream(subID, sessionID) {
  try {
    const subscribeStreamResult =
      await redis.saddAsync(redis.join(redis.SUBID_SESSIONIDS, subID), sessionID);
    if (subscribeStreamResult) debug('added %o to %o', sessionID, `${redis.SUBID_SESSIONIDS}:${subID}`);

    return subscribeStreamResult;
  } catch (error) {
    logError('subscribeStream(): %o', error);
    throw error;
  }
}

async function unsubscribeStream(subID, sessionID) {
  try {
    const unsubscribeStreamResult =
      await redis.sremAsync(redis.join(redis.SUBID_SESSIONIDS, subID), sessionID);

    return unsubscribeStreamResult;
  } catch (error) {
    logError('unsubscribeStream(): %o', error);
    throw error;
  }
}

async function unsubscribeRedis(subID) {
  try {
    const subscribeRedisResult = await redisSub.subscribeAsync(redis.join(redis.SUBID_MD, subID));
    debug('subscribeRedisResult %o', subscribeRedisResult);

    return subscribeRedisResult;
  } catch (error) {
    logError('unsubscribeRedis(): %o', error);
    throw error;
  }
}

async function removeOrphanSessionIDs(openStreamSessionIDs) {
  try {
    const [
      allSessionIDsInSubIDs,
      allSessionIDsInDataTypes,
    ] = await redis.multi()
      .keys(redis.join(redis.SUBID_SESSIONIDS, '*'))
      .keys(redis.join(redis.DATATYPE_SESSIONIDS, '*'))
      .execAsync()
      ;

    const allKeysHavingSessionIDs = allSessionIDsInSubIDs.concat(allSessionIDsInDataTypes);
    if (allKeysHavingSessionIDs.length === 0) return;

    const allRedisSessionIDs = await redis.sunionAsync(...allKeysHavingSessionIDs);

    const redisOrphanSessionIDs = difference(allRedisSessionIDs, openStreamSessionIDs);
    if (redisOrphanSessionIDs.length === 0) return;

    debug('redisOrphanSessionIDs %o', redisOrphanSessionIDs);

    const removeReport = await redis.multi(allKeysHavingSessionIDs.map(key => (['SREM', key, ...redisOrphanSessionIDs])))
      .execAsync()
      ;
    debug('removeReport %o', removeReport);
  } catch (error) {
    logError('removeOrphanSessionIDs(): %o', error);
    throw error;
  }
}

async function removeSessionIDFromAllSubIDsByDataType(sessionID, dataType) {
  try {
    const allSubIDSessionIDsKeys = await redis.keysAsync(`${redis.SUBID_SESSIONIDS}:*`);
    const allDataTypeFilteredKeys = allSubIDSessionIDsKeys.filter(elem => elem.includes(dataType));

    const isRemovedSessionID = await redis
      .multi(allDataTypeFilteredKeys.map(elem => (['SREM', elem, sessionID])))
      .execAsync()
      ;

    const removedFromSids = allDataTypeFilteredKeys.reduce((accu, curr, index) => {
      if (isRemovedSessionID[index]) accu.push(curr.substr(redis.SUBID_SESSIONIDS.length + 1));
      return accu;
    }, []);
    debug('stream %o left these rooms %o', sessionID, removedFromSids);
    return removedFromSids;
  } catch (error) {
    logError('removeSessionIDFromAllSubIDsByDataType(): %o', error);
    throw error;
  }
}

async function getSubIDsOfSessionID(sessionID) {
  try {
    const sidsKeys = await redis.keysAsync(`${redis.SUBID_SESSIONIDS}:*`);

    const isMember = await redis
      .multi(sidsKeys.map(elem => (['SISMEMBER', elem, sessionID])))
      .execAsync()
      ;

    const subIDsOfSessionID = sidsKeys.reduce((acc, cur, index) => {
      if (isMember[index]) acc.push(cur.substr(redis.SUBID_SESSIONIDS.length + 1));
      return acc;
    }, []);
    debug('subIDsOfSessionID %o', subIDsOfSessionID);

    return subIDsOfSessionID;
  } catch (error) {
    logError('getSubIDsOfSessionID(): %o', error);
    throw error;
  }
}

// async function getGloballyNotSubscribedSubIDs() {
//   try {
//     const redisResult = await redis.multi()
//       .smembers(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED))
//       .pubsub('CHANNELS', `${redis.SUBID_MD}:*`)
//       .execAsync()
//       ;
//     const globalSubIDs = redisResult[0];
//     const globallySubscribedSubIDs =
//       redisResult[1].map(elem => elem.substr(redis.SUBID_MD.length + 1));
//
//     const globallyNotSubscribedSubIDs = difference(globalSubIDs, globallySubscribedSubIDs);
//     debug('globallyNotSubscribedSubIDs %o', globallyNotSubscribedSubIDs);
//
//     return globallyNotSubscribedSubIDs;
//   } catch (error) {
//     logError('getGloballyNotSubscribedSubIDs(): %o', error);
//     throw error;
//   }
// }
//
// async function getLocallyEmptySubIDs(subIDs) {
//   try {
//     const grpcClientStreamsArr = Array.from(grpcClientStreams);
//     const allLocalStreamSessionIDs = grpcClientStreamsArr.map(elem => elem.sessionID);
//
//     const allSessionIDsInSubIDs = await redis
//       .multi(subIDs.map(subID => (['SMEMBERS', `${redis.SUBID_SESSIONIDS}:${subID}`])))
//       .execAsync()
//       ;
//
//     const locallyEmptySubIDs = allSessionIDsInSubIDs.reduce((accu, sessionids, index) => {
//       const locallySubscribed = intersection(sessionids, allLocalStreamSessionIDs);
//       if (locallySubscribed.length === 0) accu.push(subIDs[index]);
//       return accu;
//     }, []);
//
//     debug('locallyEmptySubIDs %o', locallyEmptySubIDs);
//     return locallyEmptySubIDs;
//   } catch (error) {
//     logError('getLocallyEmptySubIDs(): %o', error);
//     throw error;
//   }
// }

const subscriber = {
  subToSubID,
  subIDToSub,
  subscribeDataFeed,
  unsubscribeDataFeed,
  subscribeRedis,
  unsubscribeRedis,
  subscribeStream,
  unsubscribeStream,
  removeOrphanSessionIDs,
  removeSessionIDFromAllSubIDsByDataType,
  getSubIDsOfSessionID,
};

export default subscriber;
