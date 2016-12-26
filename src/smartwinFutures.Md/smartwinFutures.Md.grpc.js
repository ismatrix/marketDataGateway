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
const redis = createRedis.createClient({ port: 6379 });
const redisSub = redis.duplicate();

const serviceName = 'smartwinFuturesMd';

// Redis keys descriptions
const SUBID_SESSIONIDS = 'subID|sessionIDs';
const SUBID_MD = 'subID|md';
const SUBSINFO_SUBIDS = 'subsInfo|subIDs';
const SUBID_LASTMD = 'subID|lastMd';
const DATATYPE_SESSIONIDS = 'dataType|sessionIDs';

// string constants
const GLOBALLYUNUSED = 'globallyUnused';
const GLOBALLYSUBSCRIBED = 'globallySubscribed';

redis.del([SUBSINFO_SUBIDS, GLOBALLYSUBSCRIBED].join(':'));
redisSub.subscribe([SUBSINFO_SUBIDS, GLOBALLYUNUSED].join(':'));

const grpcClientStreams = new Set();

async function removeSessionIDsWithoutOpenStream() {
  try {
    const grpcClientStreamsArr = Array.from(grpcClientStreams);
    const allLocalSessionIDs = grpcClientStreamsArr.map(elem => elem.sessionID);

    const [
      allSessionIDsInSubIDs,
      allSessionIDsInDataTypes,
    ] = await redis.multi()
      .keys(`${SUBID_SESSIONIDS}:*`)
      .keys(`${DATATYPE_SESSIONIDS}:*`)
      .execAsync()
      ;

    const allKeysHavingSessionIDs = allSessionIDsInSubIDs.concat(allSessionIDsInDataTypes);
    if (allKeysHavingSessionIDs.length === 0) return;

    const allRedisSessionIDs = await redis.sunionAsync(...allKeysHavingSessionIDs);

    const orphanRedisSessionIDs = difference(allRedisSessionIDs, allLocalSessionIDs);
    if (orphanRedisSessionIDs.length === 0) return;

    debug('orphanRedisSessionIDs %o', orphanRedisSessionIDs);

    const removeReport = await redis.multi(allKeysHavingSessionIDs.map(key => (['SREM', key, ...orphanRedisSessionIDs])))
      .execAsync()
      ;
    debug('removeReport %o', removeReport);
  } catch (error) {
    logError('removeSessionIDsWithoutOpenStream(): %o', error);
    throw error;
  }
}
// On node restart, clear redisOrphanSessionIDs
setTimeout(removeSessionIDsWithoutOpenStream, 30000);

async function getGloballyNotSubscribedSubIDs() {
  try {
    const redisResult = await redis.multi()
      .smembers([SUBSINFO_SUBIDS, GLOBALLYSUBSCRIBED].join(':'))
      .pubsub('CHANNELS', `${SUBID_MD}:*`)
      .execAsync()
      ;
    const globalSubIDs = redisResult[0];
    const globallySubscribedSubIDs = redisResult[1].map(elem => elem.substr(SUBID_MD.length + 1));

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
    const allLocalStreamSessionIDs = grpcClientStreamsArr.map(elem => elem.sessionID);

    const allSessionIDsInSubIDs = await redis
      .multi(subIDs.map(subID => (['SMEMBERS', `${SUBID_SESSIONIDS}:${subID}`])))
      .execAsync()
      ;

    const locallyEmptySubIDs = allSessionIDsInSubIDs.reduce((accu, sessionids, index) => {
      const locallySubscribed = intersection(sessionids, allLocalStreamSessionIDs);
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

async function removeSessionIDFromAllSubIDsByDataType(sessionID, dataType) {
  try {
    const allSubIDSessionIDsKeys = await redis.keysAsync(`${SUBID_SESSIONIDS}:*`);
    const allDataTypeFilteredKeys = allSubIDSessionIDsKeys.filter(elem => elem.includes(dataType));

    const isRemovedSessionID = await redis
      .multi(allDataTypeFilteredKeys.map(elem => (['SREM', elem, sessionID])))
      .execAsync()
      ;

    const removedFromSids = allDataTypeFilteredKeys.reduce((accu, curr, index) => {
      if (isRemovedSessionID[index]) accu.push(curr.substr(SUBID_SESSIONIDS.length + 1));
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
    const sidsKeys = await redis.keysAsync(`${SUBID_SESSIONIDS}:*`);

    const isMember = await redis
      .multi(sidsKeys.map(elem => (['SISMEMBER', elem, sessionID])))
      .execAsync()
      ;

    const subIDsOfSessionID = sidsKeys.reduce((acc, cur, index) => {
      if (isMember[index]) acc.push(cur.substr(SUBID_SESSIONIDS.length + 1));
      return acc;
    }, []);
    debug('subIDsOfSessionID %o', subIDsOfSessionID);

    return subIDsOfSessionID;
  } catch (error) {
    logError('getSubIDsOfSessionID(): %o', error);
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

    if (roomType === SUBID_MD) {
      const subID = room.substring(room.indexOf(':') + 1);
      const subscribersSessionIDs = await redis.smembersAsync([SUBID_SESSIONIDS, subID].join(':'));

      const subscription = subIDToSubscription(subID);

      for (const stream of grpcClientStreams) {
        if (
          stream.dataType === subscription.dataType
          && subscribersSessionIDs.includes(stream.sessionID)
        ) stream.write(JSON.parse(message));
      }
    } else if (room === [SUBSINFO_SUBIDS, GLOBALLYUNUSED].join(':')) {
      const [dataFeedName] = room.split(':');
      const subToRemove = subIDToSubscription(message);
      const theDataFeed = dataFeeds.getDataFeed(dataFeedName);
      await theDataFeed.unsubscribe(subToRemove);
      const isGloballyRemoved = await redis.sremAsync([SUBSINFO_SUBIDS, GLOBALLYUNUSED].join(':'), message);
      debug('isGloballyRemoved %o', isGloballyRemoved);
    }
  } catch (error) {
    logError('redisSub.on(message): %o', error);
  }
});

function createCallID(call) {
  const sessionID = call.metadata.get('sessionid')[0];
  const peer = call.getPeer();
  const callID = `${peer}@${sessionID.substr(0, 6)}`;
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

    const sessionID = call.metadata.get('sessionid')[0];
    const newSub = call.request;
    debug('subscribeMarketData() newSub: %o, from callID: %o', newSub, betterCallID);

    const marketData = marketDatas.getMarketData(serviceName);

    const theDataFeed = marketData.getDataFeedBySubscription(newSub);

    // const grpcClientStreamsArr = Array.from(grpcClientStreams);
    // const matchingStream = grpcClientStreamsArr.find(
    //   (stream) => {
    //     if (stream.dataType === newSub.dataType && stream.sessionID === sessionID) return true;
    //     return false;
    //   });
    // if (!matchingStream) throw new Error(`Before calling subscribeMarketData(), need to first open a stream of related type "${newSub.dataType}"`);

    const subID = subscriptionToSubID(theDataFeed.config.name, newSub);

    const isNewSubInGlobal = await redis.sismemberAsync([SUBSINFO_SUBIDS, GLOBALLYSUBSCRIBED].join(':'), subID);
    if (!isNewSubInGlobal) await theDataFeed.subscribe(newSub);

    await redis.saddAsync([SUBSINFO_SUBIDS, GLOBALLYSUBSCRIBED].join(':'), subID);
    const redisSubscribed = await redisSub.subscribeAsync([SUBID_MD, subID].join(':'));
    debug('redisSubscribed %o', redisSubscribed);

    const isNewSessionIDInSubID = await redis.saddAsync(`${SUBID_SESSIONIDS}:${subID}`, sessionID);
    if (isNewSessionIDInSubID) debug('added %o to %o', sessionID, `${SUBID_SESSIONIDS}:${subID}`);

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

    const sessionID = call.metadata.get('sessionid')[0];
    const subToRemove = call.request;
    debug('unsubscribeMarketData() subToRemove: %o, from callID: %o', subToRemove, betterCallID);

    const marketData = marketDatas.getMarketData(serviceName);
    const theDataFeed = marketData.getDataFeedBySubscription(subToRemove);
    const subID = subscriptionToSubID(theDataFeed.config.name, subToRemove);

    const isRemoved = await redis.sremAsync(`${SUBID_SESSIONIDS}:${subID}`, sessionID);
    if (isRemoved) debug('removed %o from subs %o', betterCallID, subID);

    // const needRedisUnsubscribeSubIDs = await getLocallyEmptySubIDs([subID]);
    // needRedisUnsubscribeSubIDs.forEach(elem => redisSub.unsubscribe([SUBID_MD, elem].join(':')));

    // const globallyNotSubscribedSubIDs = await getGloballyNotSubscribedSubIDs();
    //
    // for (const subIDToGloballyRemove of globallyNotSubscribedSubIDs) {
    //   redis.publish([SUBSINFO_SUBIDS, GLOBALLYUNUSED].join(':'), subIDToGloballyRemove);
    // }

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
    stream.user = user;
    stream.sessionID = stream.metadata.get('sessionid')[0];

    const betterCallID = createBetterCallID(callID, stream.dataType, user.userid);
    debug('getMarketDataStream(): grpcCall from callID: %o', betterCallID);

    grpcClientStreams.forEach((existingStream) => {
      if (
        existingStream.sessionID === stream.sessionID
        && existingStream.dataType === stream.dataType
      ) throw new Error(`you already opened a similar stream of type "${stream.dataType}"`);
    });

    stream
      .on('cancelled', async () => {
        try {
          logError('stream.on(cancelled): callID: %o', betterCallID);
          grpcClientStreams.delete(stream);
          await redis.sremAsync([DATATYPE_SESSIONIDS, stream.dataType].join(':'), stream.sessionID);
          await removeSessionIDFromAllSubIDsByDataType(stream.sessionID, stream.dataType);
          // const needRedisUnsubscribeSubIDs = await getLocallyEmptySubIDs(leftSubIDs);
          // needRedisUnsubscribeSubIDs.forEach(elem => redisSub.unsubscribe([SUBID_MD, elem].join(':')));
          //
          // const globallyNotSubscribedSubIDs = await getGloballyNotSubscribedSubIDs();
          // for (const subIDToGloballyRemove of globallyNotSubscribedSubIDs) {
          //   redis.publish([SUBSINFO_SUBIDS, GLOBALLYUNUSED].join(':'), subIDToGloballyRemove);
          // }
        } catch (error) {
          logError('stream.on(cancelled): %o', error);
        }
      })
      .on('error', (error) => {
        logError('stream.on(error): callID: %o, %o', betterCallID, error);
        grpcClientStreams.delete(stream);
      })
      ;
    // debug('1');
    // await new Promise(r => setTimeout(r, 5000));
    // debug('2');
    grpcClientStreams.add(stream);
    await redis.saddAsync([DATATYPE_SESSIONIDS, stream.dataType].join(':'), stream.sessionID);

    const streamExistingSubIDs = await getSubIDsOfSessionID(stream.sessionID);
    const globalSubIDs = await redis.smembersAsync([SUBSINFO_SUBIDS, GLOBALLYSUBSCRIBED].join(':'));
    const needSubscribeSubIDs = difference(streamExistingSubIDs, globalSubIDs);
    debug('existing subIDs that need to be subscribed: %o', needSubscribeSubIDs);

    needSubscribeSubIDs.forEach(async (newSubID) => {
      try {
        const marketData = marketDatas.getMarketData(serviceName);
        const newSub = subIDToSubscription(newSubID);
        const theDataFeed = marketData.getDataFeedBySubscription(newSub);
        await theDataFeed.subscribe(newSub);
        await redis.saddAsync([SUBSINFO_SUBIDS, GLOBALLYSUBSCRIBED].join(':'), newSubID);
        const redisSubscribed = await redisSub.subscribeAsync([SUBID_MD, newSubID].join(':'));
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

    const lastRedisMarketDatas = await redis.multi(subIDs.map(subID => (['GET', [SUBID_LASTMD, subID].join(':')]))).execAsync();
    const lastMarketDatas = lastRedisMarketDatas.filter(md => !!md).map(md => JSON.parse(md));
    debug('lastMarketDatas.length %o', lastMarketDatas.length);

    const lastMarketDatasResponse = {};
    lastMarketDatasResponse[`${dataType}s`] = lastMarketDatas;

    callback(null, lastMarketDatasResponse);

    const globalSubIDs = await redis.smembersAsync([SUBSINFO_SUBIDS, GLOBALLYSUBSCRIBED].join(':'));
    const needSubscribeSubIDs = difference(subIDs, globalSubIDs);
    if (needSubscribeSubIDs.length > 0) debug('existing subIDs that need to be subscribed: %o', needSubscribeSubIDs);

    needSubscribeSubIDs.forEach(async (newSubID) => {
      try {
        const newSub = subIDToSubscription(newSubID);
        const [dataFeedName] = newSubID.split(':');
        const theDataFeed = dataFeeds.getDataFeed(dataFeedName);
        await theDataFeed.subscribe(newSub);
        debug('getLastMarketDatas(): subscribed to %o', newSubID);
        await redis.saddAsync([SUBSINFO_SUBIDS, GLOBALLYSUBSCRIBED].join(':'), newSubID);
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

    const sessionID = call.metadata.get('sessionid')[0];

    debug('getMySubscriptions(): grpcCall from callID: %o', betterCallID);

    const subIDs = await getSubIDsOfSessionID(sessionID);

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
