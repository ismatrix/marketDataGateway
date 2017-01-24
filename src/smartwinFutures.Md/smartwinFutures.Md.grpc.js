import createDebug from 'debug';
import { upperFirst, difference } from 'lodash';
import grpc from 'grpc';
import crud from 'sw-mongodb-crud';
import can from 'sw-can';
import { redis, redisSub } from '../redis';
import marketDatas from '../marketDatas';
import dataFeeds from '../dataFeeds';
import subscriber from '../subscriber';

const debug = createDebug('app:smartwinFuturesMd.grpc');
const logError = createDebug('app:smartwinFuturesMd.grpc:error');
logError.log = console.error.bind(console);

const serviceName = 'smartwinFuturesMd';

redis.del(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED));
redisSub.subscribe(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYUNUSED));

const grpcClientStreams = new Set();

// On node restart, clear orphanSessionIDs if client didn't reconnect after 1mn
const openStreamSessionIDs = Array.from(grpcClientStreams).map(elem => elem.sessionID);
setTimeout(() => subscriber.removeOrphanSessionIDs(openStreamSessionIDs), 60000);

redisSub.on('message', async (room, message) => {
  try {
    const [keyNamespace, key, dataType] = redis.getFullKeyParts(room, 'namespace', 'key', 'dataType');

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
      const [dataFeedName] = redis.getFullKeyParts(room, 'dataFeedName');
      const theDataFeed = dataFeeds.getDataFeed(dataFeedName);

      const subToRemove = subscriber.subIDToSub(message);
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
    const user = await can.grpc(stream, 'get', 'smartwinFuturesMd');

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
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');

    const betterCallID = createBetterCallID(callID, user.userid);

    const sessionID = call.metadata.get('sessionid')[0];
    const newSub = call.request;
    debug('subscribeMarketData() newSub: %o, from callID: %o', newSub, betterCallID);

    const marketData = marketDatas.getMarketData(serviceName);

    const theDataFeed = marketData.getDataFeedBySubscription(newSub);

    const subID = subscriber.subToSubID(theDataFeed.config.name, newSub);

    await subscriber.subscribeDataFeed(newSub, theDataFeed);
    await subscriber.subscribeRedis(subID);
    await subscriber.subscribeStream(subID, sessionID);

    callback(null, newSub);
  } catch (error) {
    logError('subscribeMarketData(): callID: %o, %o', callID, error);
    callback(error);
  }
}

async function unsubscribeMarketData(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');

    const betterCallID = createBetterCallID(callID, user.userid);

    const sessionID = call.metadata.get('sessionid')[0];
    const subToRemove = call.request;
    debug('unsubscribeMarketData() subToRemove: %o, from callID: %o', subToRemove, betterCallID);

    const marketData = marketDatas.getMarketData(serviceName);
    const theDataFeed = marketData.getDataFeedBySubscription(subToRemove);

    const subID = subscriber.subToSubID(theDataFeed.config.name, subToRemove);

    subscriber.unsubscribeStream(subID, sessionID);

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
    const user = await can.grpc(stream, 'get', 'smartwinFuturesMd');
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
          await subscriber.removeSessionIDFromAllSubIDsByDataType(
            stream.sessionID, stream.dataType);
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

    // const metadataUpdater = (serviceUrl, callback) => {
    //   const metadata = new grpc.Metadata();
    //   metadata.set('callback', 'ok');
    //   callback(null, metadata);
    // };
    // const metadataCreds = grpc.credentials.createFromMetadataGenerator(metadataUpdater);
    const metadataCB = new grpc.Metadata();
    metadataCB.add('ok', 'true');
    stream.sendMetadata(metadataCB);

    const streamExistingSubIDs = await subscriber.getSubIDsOfSessionID(stream.sessionID);
    const globalSubIDs = await redis.smembersAsync(
      redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED));
    const needSubscribeSubIDs = difference(streamExistingSubIDs, globalSubIDs);
    debug('existing subIDs that need to be subscribed: %o', needSubscribeSubIDs);

    needSubscribeSubIDs.forEach(async (newSubID) => {
      try {
        const marketData = marketDatas.getMarketData(serviceName);
        const newSub = subscriber.subIDToSub(newSubID);
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
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');
    const betterCallID = createBetterCallID(callID, user.userid);
    debug('%o(): grpcCall from callID: %o', methodName, betterCallID);

    for (const sub of subs) {
      if (sub.dataType !== dataType) throw new Error(`cannot ask for dataType: "${sub.dataType}" with method: "${methodName}"`);
    }

    if (subs.length === 0) return callback(null, {});

    const marketData = marketDatas.getMarketData(serviceName);

    const subIDs = subs.map((sub) => {
      const dataFeedName = marketData.getDataFeedBySubscription(sub).config.name;
      return subscriber.subToSubID(dataFeedName, sub);
    });
    debug('subIDs %o', subIDs);

    const lastRedisMarketDatas =
      await redis.mgetAsync(subIDs.map(subID => redis.join(redis.SUBID_LASTMD, subID)));
    const lastMarketDatas = lastRedisMarketDatas.filter(md => !!md).map(md => JSON.parse(md));
    debug('lastMarketDatas.length %o', lastMarketDatas.length);

    const lastMarketDatasResponse = {};
    lastMarketDatasResponse[`${dataType}s`] = lastMarketDatas;

    callback(null, lastMarketDatasResponse);

    const globalSubIDs =
      await redis.smembersAsync(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED));
    const needSubscribeSubIDs = difference(subIDs, globalSubIDs);
    if (needSubscribeSubIDs.length > 0) debug('existing subIDs that need to be subscribed: %o', needSubscribeSubIDs);

    needSubscribeSubIDs.forEach(async (newSubID) => {
      try {
        const newSub = subscriber.subIDToSub(newSubID);
        const [dataFeedName] = redis.getKeyParts(redis.SUBID, newSubID, 'dataFeedName');

        const theDataFeed = dataFeeds.getDataFeed(dataFeedName);

        await subscriber.subscribeDataFeed(newSub, theDataFeed);
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
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');
    const betterCallID = createBetterCallID(callID, user.userid);
    debug('getInstruments() request: %o, grpcCall from callID: %o', call.request, betterCallID);

    const req = call.request;
    const filter = {};

    if ('symbols' in req && req.symbols.length && !req.symbols.includes('all')) filter.instruments = req.symbols;
    if ('products' in req && req.products.length && !req.products.includes('all')) filter.product = req.products;
    if ('exchanges' in req && req.exchanges.length && !req.exchanges.includes('all')) filter.exchange = req.exchanges;
    if ('ranks' in req && req.ranks.length && !req.ranks.includes('all')) filter.rank = req.ranks;
    if ('productClasses' in req && req.productClasses.length && !req.productClasses.includes('all')) filter.productclass = req.productClasses;
    if ('isTrading' in req && req.isTrading.length && !req.isTrading.includes('all')) filter.istrading = req.isTrading;

    const dbInstruments = await crud.instrument.getList(filter);

    // db instrumentname === null not compatible with proto3
    const instruments = dbInstruments.map((ins) => {
      if (ins.instrumentname === null) delete ins.instrumentname;
      return ins;
    });

    callback(null, { instruments });
  } catch (error) {
    logError('getInstruments(): callID: %o, %o', callID, error);
    callback(error);
  }
}

async function getSubscribableDataDescriptions(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');
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
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');
    const betterCallID = createBetterCallID(callID, user.userid);

    const sessionID = call.metadata.get('sessionid')[0];

    debug('getMySubscriptions(): grpcCall from callID: %o', betterCallID);

    const subIDs = await subscriber.getSubIDsOfSessionID(sessionID);

    const subscriptions = subIDs.map(subID => subscriber.subIDToSub(subID));

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
