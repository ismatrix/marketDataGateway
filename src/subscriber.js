import { difference } from 'lodash';
import { redis, redisSub } from './redis';
import logger from 'sw-common'

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
    logger.error('subscribeDataFeed(): %j', error);
    throw error;
  }
}

async function unsubscribeDataFeed(sub, dataFeed) {
  try {
    logger.info('to be completed %j %j', sub, dataFeed);

    return sub;
  } catch (error) {
    logger.error('unsubscribeDataFeed(): %j', error);
    throw error;
  }
}

async function subscribeRedis(subID) {
  try {
    const subscribeRedisResult = await redisSub.subscribeAsync(redis.join(redis.SUBID_MD, subID));
    logger.info('subscribeRedisResult %j', subscribeRedisResult);

    return subscribeRedisResult;
  } catch (error) {
    logger.error('subscribeRedis(): %j', error);
    throw error;
  }
}

async function subscribeStream(subID, sessionID) {
  try {
    const subscribeStreamResult =
      await redis.saddAsync(redis.join(redis.SUBID_SESSIONIDS, subID), sessionID);
    if (subscribeStreamResult) logger.info('added %j to %j', sessionID, redis.join(redis.SUBID_SESSIONIDS, subID));

    return subscribeStreamResult;
  } catch (error) {
    logger.error('subscribeStream(): %j', error);
    throw error;
  }
}

async function unsubscribeStream(subID, sessionID) {
  try {
    const unsubscribeStreamResult =
      await redis.sremAsync(redis.join(redis.SUBID_SESSIONIDS, subID), sessionID);

    return unsubscribeStreamResult;
  } catch (error) {
    logger.error('unsubscribeStream(): %j', error);
    throw error;
  }
}

async function unsubscribeRedis(subID) {
  try {
    const subscribeRedisResult = await redisSub.subscribeAsync(redis.join(redis.SUBID_MD, subID));
    logger.info('subscribeRedisResult %j', subscribeRedisResult);

    return subscribeRedisResult;
  } catch (error) {
    logger.error('unsubscribeRedis(): %j', error);
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

    logger.info('redisOrphanSessionIDs %j', redisOrphanSessionIDs);

    const removeReport = await redis.multi(allKeysHavingSessionIDs.map(key => (['SREM', key, ...redisOrphanSessionIDs])))
      .execAsync()
      ;
    logger.info('removeReport %j', removeReport);
  } catch (error) {
    logger.error('removeOrphanSessionIDs(): %j', error);
    throw error;
  }
}

async function removeSessionIDFromAllSubIDsByDataType(sessionID, dataType) {
  try {
    const allSubIDSessionIDsKeys = await redis.keysAsync(redis.join(redis.SUBID_SESSIONIDS, '*'));
    const allDataTypeFilteredKeys = allSubIDSessionIDsKeys.filter(elem => elem.includes(dataType));

    const isRemovedSessionID = await redis
      .multi(allDataTypeFilteredKeys.map(elem => (['SREM', elem, sessionID])))
      .execAsync()
      ;

    const removedFromSids = allDataTypeFilteredKeys.reduce((accu, curr, index) => {
      if (isRemovedSessionID[index]) accu.push(curr.substr(redis.SUBID_SESSIONIDS.length + 1));
      return accu;
    }, []);
    logger.info('stream %j left these rooms %j', sessionID, removedFromSids);
    return removedFromSids;
  } catch (error) {
    logger.error('removeSessionIDFromAllSubIDsByDataType(): %j', error);
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
    logger.info('subIDsOfSessionID %j', subIDsOfSessionID);

    return subIDsOfSessionID;
  } catch (error) {
    logger.error('getSubIDsOfSessionID(): %j', error);
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
//     logger.info('globallyNotSubscribedSubIDs %j', globallyNotSubscribedSubIDs);
//
//     return globallyNotSubscribedSubIDs;
//   } catch (error) {
//     logger.error('getGloballyNotSubscribedSubIDs(): %j', error);
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
//     logger.info('locallyEmptySubIDs %j', locallyEmptySubIDs);
//     return locallyEmptySubIDs;
//   } catch (error) {
//     logger.error('getLocallyEmptySubIDs(): %j', error);
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
