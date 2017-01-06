import createDebug from 'debug';
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

const subscriber = {
  subToSubID,
  subIDToSub,
  subscribeDataFeed,
  unsubscribeDataFeed,
  subscribeRedis,
  unsubscribeRedis,
  subscribeStream,
  unsubscribeStream,
};

export default subscriber;
