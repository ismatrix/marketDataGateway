import createDebug from 'debug';
import { upperFirst, difference } from 'lodash';
import { redis, redisSub } from '../redis';
import marketDatas from '../marketDatas';
import dataFeeds from '../dataFeeds';
import grpcCan from '../acl';

const debug = createDebug('app:smartwinFuturesMd.grpc');
const logError = createDebug('app:smartwinFuturesMd.grpc:error');
logError.log = console.error.bind(console);

const serviceName = 'smartwinFuturesMd';

redis.del(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED));
redisSub.subscribe(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYUNUSED));

const grpcClientStreams = new Set();

async function removeSessionIDsWithoutOpenStream() {
  try {
    const grpcClientStreamsArr = Array.from(grpcClientStreams);
    const allLocalSessionIDs = grpcClientStreamsArr.map(elem => elem.sessionID);

    const [
      allSessionIDsInSubIDs,
      allSessionIDsInDataTypes,
    ] = await redis.multi()
      .keys(`${redis.SUBID_SESSIONIDS}:*`)
      .keys(`${redis.DATATYPE_SESSIONIDS}:*`)
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

function subIDToObject(subID) {
  const [dataFeedName, dataType, resolution, symbol] = subID.split(redis.SUBKEYSSEP);
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
  const subID = redis.joinSubKeys(dataFeedName, sub.dataType, sub.resolution, sub.symbol);
  return subID;
}

redisSub.on('message', async (room, message) => {
  try {
    const [keyNamespace, key, dataType] = redis.getSubKeysByNames(room, 'namespace', 'key', 'dataType');

    if (keyNamespace === redis.SUBID_MD) {
      const subID = key;

      const subscribersSessionIDs =
        await redis.smembersAsync(redis.join(redis.SUBID_SESSIONIDS, subID));

      for (const stream of grpcClientStreams) {
        if (
          stream.dataType === dataType
          && subscribersSessionIDs.includes(stream.sessionID)
        ) stream.write(JSON.parse(message));
      }
    } else if (keyNamespace === redis.SUBSINFO_SUBIDS && key === redis.GLOBALLYUNUSED) {
      const [dataFeedName] = redis.getSubKeysByNames(room, 'dataFeedName');
      const theDataFeed = dataFeeds.getDataFeed(dataFeedName);

      const subToRemove = subIDToSubscription(message);
      await theDataFeed.unsubscribe(subToRemove);

      const isGloballyRemoved = await redis.sremAsync(
        redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYUNUSED), message);
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

    const subID = subscriptionToSubID(theDataFeed.config.name, newSub);

    const isNewSubInGlobal = await redis.sismemberAsync(
      redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED), subID);
    if (!isNewSubInGlobal) await theDataFeed.subscribe(newSub);

    await redis.saddAsync(
      redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED), subID);
    const redisSubscribed = await redisSub.subscribeAsync(redis.join(redis.SUBID_MD, subID));
    debug('redisSubscribed %o', redisSubscribed);

    const isNewSessionIDInSubID =
      await redis.saddAsync(redis.join(redis.SUBID_SESSIONIDS, subID), sessionID);
    if (isNewSessionIDInSubID) debug('added %o to %o', sessionID, `${redis.SUBID_SESSIONIDS}:${subID}`);

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

    const isRemoved =
      await redis.sremAsync(redis.join(redis.SUBID_SESSIONIDS, subID), sessionID);
    if (isRemoved) debug('removed %o from subs %o', betterCallID, subID);

    // const needRedisUnsubscribeSubIDs = await getLocallyEmptySubIDs([subID]);
    // needRedisUnsubscribeSubIDs.forEach(elem =>
    // redisSub.unsubscribe(redis.join(redis.SUBID_MD, elem));

    // const globallyNotSubscribedSubIDs = await getGloballyNotSubscribedSubIDs();
    //
    // for (const subIDToGloballyRemove of globallyNotSubscribedSubIDs) {
    //   redis.publish(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYUNUSED),
    // subIDToGloballyRemove);
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
          await redis.sremAsync(
            redis.join(redis.DATATYPE_SESSIONIDS, stream.dataType), stream.sessionID);
          await removeSessionIDFromAllSubIDsByDataType(stream.sessionID, stream.dataType);
          // const needRedisUnsubscribeSubIDs = await getLocallyEmptySubIDs(leftSubIDs);
          // needRedisUnsubscribeSubIDs.forEach(elem =>
          // redisSub.unsubscribe(redis.join(redis.SUBID_MD, elem));
          //
          // const globallyNotSubscribedSubIDs = await getGloballyNotSubscribedSubIDs();
          // for (const subIDToGloballyRemove of globallyNotSubscribedSubIDs) {
          //   redis.publish(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYUNUSED),
          // subIDToGloballyRemove);
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

    grpcClientStreams.add(stream);
    await redis.saddAsync(redis.join(redis.DATATYPE_SESSIONIDS, stream.dataType), stream.sessionID);

    const streamExistingSubIDs = await getSubIDsOfSessionID(stream.sessionID);
    const globalSubIDs = await redis.smembersAsync(
      redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED));
    const needSubscribeSubIDs = difference(streamExistingSubIDs, globalSubIDs);
    debug('existing subIDs that need to be subscribed: %o', needSubscribeSubIDs);

    needSubscribeSubIDs.forEach(async (newSubID) => {
      try {
        const marketData = marketDatas.getMarketData(serviceName);
        const newSub = subIDToSubscription(newSubID);
        const theDataFeed = marketData.getDataFeedBySubscription(newSub);
        await theDataFeed.subscribe(newSub);
        await redis.saddAsync(
          redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED), newSubID);
        await redisSub.subscribeAsync(redis.join(redis.SUBID_MD, newSubID));
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

    if (subs.length === 0) return callback(null, {});

    const marketData = marketDatas.getMarketData(serviceName);

    const subIDs = subs.map((sub) => {
      const dataFeedName = marketData.getDataFeedBySubscription(sub).config.name;
      return subscriptionToSubID(dataFeedName, sub);
    });
    debug('subIDs %o', subIDs);

    const lastRedisMarketDatas =
      await redis.mgetAsync(subIDs.map(subID => redis.join(redis.SUBID_LASTMD, subID)));
    const lastMarketDatas = lastRedisMarketDatas.filter(md => !!md).map(md => JSON.parse(md));
    debug('lastMarketDatas.length %o', lastMarketDatas.length);

    const lastMarketDatasResponse = {};
    lastMarketDatasResponse[`${dataType}s`] = lastMarketDatas;

    callback(null, lastMarketDatasResponse);
    debug('callback(lastMarketDatasResponse)');

    const globalSubIDs =
      await redis.smembersAsync(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED));
    const needSubscribeSubIDs = difference(subIDs, globalSubIDs);
    if (needSubscribeSubIDs.length > 0) debug('existing subIDs that need to be subscribed: %o', needSubscribeSubIDs);

    needSubscribeSubIDs.forEach(async (newSubID) => {
      try {
        const newSub = subIDToSubscription(newSubID);
        const [dataFeedName] = newSubID.split(redis.SUBKEYSSEP);
        const theDataFeed = dataFeeds.getDataFeed(dataFeedName);
        await theDataFeed.subscribe(newSub);
        debug('getLastMarketDatas(): subscribed to %o', newSubID);
        await redis.saddAsync(
          redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED), newSubID);
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
