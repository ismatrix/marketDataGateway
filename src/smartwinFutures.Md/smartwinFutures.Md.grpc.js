import createDebug from 'debug';
import { upperFirst, intersection, difference } from 'lodash';
import bluebird from 'bluebird';
import createRedis from 'redis';
import marketDatas from '../marketDatas';
import dataFeeds from '../dataFeeds';
import grpcCan from '../acl';

const debug = createDebug('app:smartwinFuturesMd.grpc');
const logError = createDebug('app:smartwinFuturesMd.grpc:error');
logError.log = console.error.bind(console);
bluebird.promisifyAll(createRedis.RedisClient.prototype);
bluebird.promisifyAll(createRedis.Multi.prototype);
const redis = createRedis.createClient();
const redisSub = redis.duplicate();

const serviceName = 'smartwinFuturesMd';
const SIDS_PREFIX = 'sids';
const GLOBAL_SUBS = 'subs:global';
const MD_ROOM = 'md';
const GLOBALLY_UNUSED_SUBID = 'mdSubID:globallyUnused';
const LAST_MD = 'subid.lastMd';

redis.del(GLOBAL_SUBS);
redisSub.subscribe(GLOBALLY_UNUSED_SUBID);

const grpcClientStreams = new Set();

async function getGloballyNotSubscribedSubIDs() {
  try {
    const redisResult = await redis.multi()
      .smembers(GLOBAL_SUBS)
      .pubsub('CHANNELS', `${MD_ROOM}:*`)
      .execAsync()
      ;
    const globalSubIDs = redisResult[0];
    const globallySubscribedSubIDs = redisResult[1].map(elem => elem.substr(MD_ROOM.length + 1));

    const globallyNotSubscribedSubIDs = difference(globalSubIDs, globallySubscribedSubIDs);
    debug('globallyNotSubscribedSubIDs %o', globallyNotSubscribedSubIDs);

    return globallyNotSubscribedSubIDs;
  } catch (error) {
    logError('getGloballyNotSubscribedSubIDs(): %o', error);
    throw error;
  }
}

async function getLocallyEmptySubIDs(subIDs) {
  try {
    const grpcClientStreamsArr = Array.from(grpcClientStreams);
    const allLocalStreamSessionids = grpcClientStreamsArr.map(elem => elem.sessionid);

    const allSessionidsInSubIDs = await redis
      .multi(subIDs.map(subID => (['SMEMBERS', `${SIDS_PREFIX}:${subID}`])))
      .execAsync()
      ;

    const locallyEmptySubIDs = allSessionidsInSubIDs.reduce((accu, sessionids, index) => {
      const locallySubscribed = intersection(sessionids, allLocalStreamSessionids);
      if (locallySubscribed.length === 0) accu.push(subIDs[index]);
      return accu;
    }, []);

    debug('locallyEmptySubIDs %o', locallyEmptySubIDs);
    return locallyEmptySubIDs;
  } catch (error) {
    logError('getLocallyEmptySubIDs(): %o', error);
    throw error;
  }
}

async function removeSessionidFromAllSubIDsByDataType(sessionid, dataType) {
  try {
    const allSidsKeys = await redis.keysAsync(`${SIDS_PREFIX}:*`);
    const allDataTypeSidsKeys = allSidsKeys.filter(elem => elem.includes(dataType));

    const isRemovedSessionid = await redis
      .multi(allDataTypeSidsKeys.map(elem => (['SREM', elem, sessionid])))
      .execAsync()
      ;

    const removedFromSids = allDataTypeSidsKeys.reduce((accu, curr, index) => {
      if (isRemovedSessionid[index]) accu.push(curr.substr(SIDS_PREFIX.length + 1));
      return accu;
    }, []);
    debug('stream %o left these rooms %o', sessionid, removedFromSids);
    return removedFromSids;
  } catch (error) {
    logError('removeSessionidFromAllSubIDsByDataType(): %o', error);
    throw error;
  }
}

async function getSubIDsOfSessionid(sessionid) {
  try {
    const sidsKeys = await redis.keysAsync(`${SIDS_PREFIX}:*`);
    debug('sidsKeys: %o', sidsKeys);

    const isMember = await redis
      .multi(sidsKeys.map(elem => (['SISMEMBER', elem, sessionid])))
      .execAsync()
      ;

    const sessionidSubIDs = sidsKeys.reduce((acc, cur, index) => {
      if (isMember[index]) acc.push(cur.substr(SIDS_PREFIX.length + 1));
      return acc;
    }, []);
    debug('sessionidSubIDs %o', sessionidSubIDs);

    return sessionidSubIDs;
  } catch (error) {
    logError('getSubIDsOfSessionid(): %o', error);
    throw error;
  }
}

function subIDToObject(subID) {
  const [dataFeedName, dataType, resolution, symbol] = subID.split(':');
  const subIDObject = {
    dataFeedName,
    dataType,
    resolution,
    symbol,
  };
  return subIDObject;
}

function subIDToSubscription(subID) {
  const {
    dataType,
    resolution,
    symbol,
  } = subIDToObject(subID);
  const subscription = {
    dataType,
    resolution,
    symbol,
  };
  return subscription;
}

function subscriptionToSubID(dataFeedName, sub) {
  const subID = `${dataFeedName}:${sub.dataType}:${sub.resolution}:${sub.symbol}`;
  return subID;
}

redisSub.on('message', async (room, message) => {
  try {
    const roomType = room.substring(0, room.indexOf(':'));

    if (roomType === MD_ROOM) {
      const subID = room.substring(room.indexOf(':') + 1);
      const subscribersSessionids = await redis.smembersAsync([SIDS_PREFIX, subID].join(':'));

      const subscription = subIDToSubscription(subID);

      for (const stream of grpcClientStreams) {
        if (
          stream.dataType === subscription.dataType
          && subscribersSessionids.includes(stream.sessionid)
        ) stream.write(JSON.parse(message));
      }
    } else if (room === GLOBALLY_UNUSED_SUBID) {
      const [dataFeedName] = room.split(':');
      const subToRemove = subIDToSubscription(message);
      const theDataFeed = dataFeeds.getDataFeed(dataFeedName);
      await theDataFeed.unsubscribe(subToRemove);
      const isGloballyRemoved = await redis.sremAsync(GLOBAL_SUBS, message);
      debug('isGloballyRemoved %o', isGloballyRemoved);
    }
  } catch (error) {
    logError('redisSub.on(message): %o', error);
  }
});

function createCallID(call) {
  const sessionid = call.metadata.get('sessionid')[0];
  const peer = call.getPeer();
  const callID = `${peer}@${sessionid.substr(0, 6)}`;
  return callID;
}

function createBetterCallID(callID, ...args) {
  return [callID, ...args].join('@');
}

async function getPastMarketDataStream(stream) {
  const callID = createCallID(stream);
  try {
    const user = await grpcCan(stream, 'read', 'getOrders');

    const betterCallID = createBetterCallID(callID, user.userid, stream.request.dataType);
    debug('getPastMarketDataStream(): grpcCall from callID: %o', betterCallID);
    debug('getPastMarketDataStream(): grpcCall request: %o', stream.request);

    const marketData = marketDatas.getMarketData(serviceName);
    const theDataFeed = marketData.getDataFeedBySubscription(stream.request);

    theDataFeed.getPastMarketDataStream(stream.request)
      .on('error', (error) => {
        logError('getPastMarketDataStream.on(error): callID: %o, %o', betterCallID, error);
        stream.emit('error', error);
      })
      .on('end', () => debug('end of pastMarketData stream with callID: %o', betterCallID))
      .pipe(stream)
      .on('error', (error) => {
        logError('stream.on(error): callID: %o, %o', betterCallID, error);
      })
      ;
  } catch (error) {
    logError('getPastMarketDataStream(): callID: %o, %o', callID, error);
    throw error;
  }
}

async function getPastMarketDepthStream(stream) {
  try {
    stream.request.dataType = 'marketDepth';
    await getPastMarketDataStream(stream);
  } catch (error) {
    logError('getPastMarketDepthStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getPastBarStream(stream) {
  try {
    stream.request.dataType = 'bar';
    await getPastMarketDataStream(stream);
  } catch (error) {
    logError('getPastBarStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getPastTickerStream(stream) {
  try {
    stream.request.dataType = 'ticker';
    await getPastMarketDataStream(stream);
  } catch (error) {
    logError('getPastTickerStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getPastDayBarStream(stream) {
  try {
    stream.request.dataType = 'dayBar';
    await getPastMarketDataStream(stream);
  } catch (error) {
    logError('getPastDayBarStream(): %o', error);
    stream.emit('error', error);
  }
}

async function subscribeMarketData(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await grpcCan(call, 'read', 'getOrders');

    const betterCallID = createBetterCallID(callID, user.userid);

    const sessionid = call.metadata.get('sessionid')[0];
    const newSub = call.request;
    debug('subscribeMarketData() newSub: %o, from callID: %o', newSub, betterCallID);

    const marketData = marketDatas.getMarketData(serviceName);

    const theDataFeed = marketData.getDataFeedBySubscription(newSub);

    const grpcClientStreamsArr = Array.from(grpcClientStreams);
    const matchingStream = grpcClientStreamsArr.find(
      (stream) => {
        if (stream.dataType === newSub.dataType && stream.sessionid === sessionid) return true;
        return false;
      });
    if (!matchingStream) throw new Error(`Before calling subscribeMarketData(), need to first open a stream of related type "${newSub.dataType}"`);

    const subID = subscriptionToSubID(theDataFeed.config.name, newSub);

    const isNewSubInGlobal = await redis.sismemberAsync(GLOBAL_SUBS, subID);
    if (!isNewSubInGlobal) await theDataFeed.subscribe(newSub);

    await redis.saddAsync(GLOBAL_SUBS, subID);
    await redisSub.subscribeAsync([MD_ROOM, subID].join(':'));

    const isNewSessionidInSubID = await redis.saddAsync(`${SIDS_PREFIX}:${subID}`, sessionid);
    if (isNewSessionidInSubID) debug('added %o to %o', sessionid, `${SIDS_PREFIX}:${subID}`);

    callback(null, newSub);
  } catch (error) {
    logError('subscribeMarketData(): callID: %o, %o', callID, error);
    callback(error);
  }
}

async function unsubscribeMarketData(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await grpcCan(call, 'read', 'getOrders');

    const betterCallID = createBetterCallID(callID, user.userid);

    const sessionid = call.metadata.get('sessionid')[0];
    const subToRemove = call.request;
    debug('unsubscribeMarketData() subToRemove: %o, from callID: %o', subToRemove, betterCallID);

    const marketData = marketDatas.getMarketData(serviceName);
    const theDataFeed = marketData.getDataFeedBySubscription(subToRemove);
    const subID = subscriptionToSubID(theDataFeed.config.name, subToRemove);

    const isRemoved = await redis.sremAsync(`${SIDS_PREFIX}:${subID}`, sessionid);
    if (isRemoved) debug('removed %o from subs %o', betterCallID, subID);

    const needRedisUnsubscribeSubIDs = await getLocallyEmptySubIDs([subID]);
    needRedisUnsubscribeSubIDs.forEach(elem => redisSub.unsubscribe([MD_ROOM, elem].join(':')));

    const globallyNotSubscribedSubIDs = await getGloballyNotSubscribedSubIDs();

    for (const subIDToGloballyRemove of globallyNotSubscribedSubIDs) {
      redis.publish(GLOBALLY_UNUSED_SUBID, subIDToGloballyRemove);
    }

    callback(null, subToRemove);
  } catch (error) {
    logError('unsubscribeMarketData(): callID: %o, %o', callID, error);
    callback(error);
  }
}

async function getMarketDataStream(stream) {
  const callID = createCallID(stream);
  try {
    const user = await grpcCan(stream, 'read', 'getOrders');
    stream.sessionid = stream.metadata.get('sessionid')[0];

    const betterCallID = createBetterCallID(callID, stream.dataType, user.userid);
    debug('getMarketDataStream(): grpcCall from callID: %o', betterCallID);

    grpcClientStreams.forEach((existingStream) => {
      if (
        existingStream.sessionid === stream.sessionid
        && existingStream.dataType === stream.dataType
      ) throw new Error(`you already opened a similar stream of type "${stream.dataType}"`);
    });

    stream
      .on('cancelled', async () => {
        try {
          logError('stream.on(cancelled): callID: %o', betterCallID);
          grpcClientStreams.delete(stream);
          const leftSubIDs = await
            removeSessionidFromAllSubIDsByDataType(stream.sessionid, stream.dataType);
          const needRedisUnsubscribeSubIDs = await getLocallyEmptySubIDs(leftSubIDs);
          needRedisUnsubscribeSubIDs.forEach(elem => redisSub.unsubscribe([MD_ROOM, elem].join(':')));

          const globallyNotSubscribedSubIDs = await getGloballyNotSubscribedSubIDs();
          for (const subIDToGloballyRemove of globallyNotSubscribedSubIDs) {
            redis.publish(GLOBALLY_UNUSED_SUBID, subIDToGloballyRemove);
          }
        } catch (error) {
          logError('stream.on(cancelled): %o', error);
        }
      })
      .on('error', (error) => {
        logError('stream.on(error): callID: %o, %o', betterCallID, error);
        grpcClientStreams.delete(stream);
      })
      ;

    grpcClientStreams.add(stream);

    const streamExistingSubIDs = await getSubIDsOfSessionid(stream.sessionid);
    const globalSubIDs = await redis.smembersAsync(GLOBAL_SUBS);
    const needSubscribeSubIDs = difference(streamExistingSubIDs, globalSubIDs);
    debug('existing subIDs that need to be subscribed: %o', needSubscribeSubIDs);

    needSubscribeSubIDs.forEach(async (newSubID) => {
      try {
        const marketData = marketDatas.getMarketData(serviceName);
        const newSub = subIDToSubscription(newSubID);
        const theDataFeed = marketData.getDataFeedBySubscription(newSub);
        await theDataFeed.subscribe(newSub);
        await redis.saddAsync(GLOBAL_SUBS, newSubID);
        await redisSub.subscribeAsync([MD_ROOM, newSubID].join(':'));
      } catch (error) {
        logError('needSubscribeSubIDs.forEach(): %o', error);
      }
    });
  } catch (error) {
    logError('getMarketDataStream(): callID: %o, %o', callID, error);
    throw error;
  }
}

async function getMarketDepthStream(stream) {
  try {
    stream.dataType = 'marketDepth';
    await getMarketDataStream(stream);
  } catch (error) {
    logError('getMarketDepthStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getBarStream(stream) {
  try {
    stream.dataType = 'bar';
    await getMarketDataStream(stream);
  } catch (error) {
    logError('getBarStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getTickerStream(stream) {
  try {
    stream.dataType = 'ticker';
    await getMarketDataStream(stream);
  } catch (error) {
    logError('getTickerStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getDayBarStream(stream) {
  try {
    stream.dataType = 'dayBar';
    await getMarketDataStream(stream);
  } catch (error) {
    logError('getDayBarStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getLastMarketDatas(call, callback) {
  const callID = createCallID(call);
  try {
    const dataType = call.request.dataType;
    const subs = call.request.subscriptions;
    const methodName = `getLast${upperFirst(dataType)}s`;
    const user = await grpcCan(call, 'read', 'getOrders');
    const betterCallID = createBetterCallID(callID, user.userid);
    debug('%o(): grpcCall from callID: %o', methodName, betterCallID);

    for (const sub of subs) {
      if (sub.dataType !== dataType) throw new Error(`cannot ask for dataType: "${sub.dataType}" with method: "${methodName}"`);
    }

    const marketData = marketDatas.getMarketData(serviceName);

    const subIDs = subs.map((sub) => {
      const dataFeedName = marketData.getDataFeedBySubscription(sub).config.name;
      return subscriptionToSubID(dataFeedName, sub);
    });
    debug('subIDs %o', subIDs);

    const lastRedisMarketDatas = await redis.multi(subIDs.map(subID => (['GET', [LAST_MD, subID].join(':')]))).execAsync();
    const lastMarketDatas = lastRedisMarketDatas.filter(md => !!md).map(md => JSON.parse(md));
    debug('lastMarketDatas %o', lastMarketDatas);

    const lastMarketDatasResponse = {};
    lastMarketDatasResponse[`${dataType}s`] = lastMarketDatas;

    callback(null, lastMarketDatasResponse);

    const globalSubIDs = await redis.smembersAsync(GLOBAL_SUBS);
    const needSubscribeSubIDs = difference(subIDs, globalSubIDs);
    debug('existing subIDs that need to be subscribed: %o', needSubscribeSubIDs);

    needSubscribeSubIDs.forEach(async (newSubID) => {
      try {
        const newSub = subIDToSubscription(newSubID);
        const [dataFeedName] = newSubID.split(':');
        const theDataFeed = dataFeeds.getDataFeed(dataFeedName);
        await theDataFeed.subscribe(newSub);
        await redis.saddAsync(GLOBAL_SUBS, newSubID);
      } catch (error) {
        logError('needSubscribeSubIDs.forEach(): %o', error);
      }
    });
  } catch (error) {
    logError('getLastMarketDatas(): callID: %o, %o', callID, error);
    callback(error);
  }
}

async function getLastMarketDepths(call, callback) {
  try {
    call.request.dataType = 'marketDepth';
    await getLastMarketDatas(call, callback);
  } catch (error) {
    logError('getLastMarketDepths(): %o', error);
    callback(error);
  }
}

async function getLastBars(call, callback) {
  try {
    call.request.dataType = 'bar';
    await getLastMarketDatas(call, callback);
  } catch (error) {
    logError('getLastBars(): %o', error);
    callback(error);
  }
}

async function getLastTickers(call, callback) {
  try {
    call.request.dataType = 'ticker';
    await getLastMarketDatas(call, callback);
  } catch (error) {
    logError('getLastTickers(): %o', error);
    callback(error);
  }
}

async function getLastDayBars(call, callback) {
  try {
    call.request.dataType = 'dayBar';
    await getLastMarketDatas(call, callback);
  } catch (error) {
    logError('getLastTickers(): %o', error);
    callback(error);
  }
}

async function getInstruments(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await grpcCan(call, 'read', 'getOrders');
    const betterCallID = createBetterCallID(callID, user.userid);

    debug('getInstruments() symbols: %o, grpcCall from callID: %o', call.request.symbols, betterCallID);
    const marketData = marketDatas.getMarketData(serviceName);

    const instruments = await marketData.getInstruments(call.request.symbols);
    // debug('instruments %o', instruments);
    callback(null, { instruments });
  } catch (error) {
    logError('getInstruments(): callID: %o, %o', callID, error);
    callback(error);
  }
}

async function getSubscribableDataDescriptions(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await grpcCan(call, 'read', 'getOrders');
    const betterCallID = createBetterCallID(callID, user.userid);

    debug('getSubscribableDataDescriptions(): grpcCall from callID: %o', betterCallID);
    const marketData = marketDatas.getMarketData(serviceName);

    const subscribableDataDescriptions = await marketData.getSubscribableDataDescriptions();

    callback(null, { subscribableDataDescriptions });
  } catch (error) {
    logError('getSubscribableDataDescriptions(): callID: %o, %o', callID, error);
    callback(error);
  }
}

async function getMySubscriptions(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await grpcCan(call, 'read', 'getOrders');
    const betterCallID = createBetterCallID(callID, user.userid);

    const sessionid = call.metadata.get('sessionid')[0];

    debug('getMySubscriptions(): grpcCall from callID: %o', betterCallID);

    const subIDs = await getSubIDsOfSessionid(sessionid);

    const subscriptions = subIDs.map(subID => subIDToSubscription(subID));

    callback(null, { subscriptions });
  } catch (error) {
    logError('getMySubscriptions(): callID: %o, %o', callID, error);
    callback(error);
  }
}

const smartwinFutures = {
  getMarketDepthStream,
  getBarStream,
  getTickerStream,
  getDayBarStream,

  getPastMarketDepthStream,
  getPastBarStream,
  getPastTickerStream,
  getPastDayBarStream,

  subscribeMarketData,
  unsubscribeMarketData,

  getLastMarketDepths,
  getLastBars,
  getLastTickers,
  getLastDayBars,

  getInstruments,

  getSubscribableDataDescriptions,
  getMySubscriptions,
};

export default smartwinFutures;
