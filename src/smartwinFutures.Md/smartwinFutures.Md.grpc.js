import logger from 'sw-common';
import { upperFirst, difference } from 'lodash';
import grpc from 'grpc';
import can from 'sw-can';
import { redis, redisSub } from '../redis';
import marketDatas from '../marketDatas';
import dataFeeds from '../dataFeeds';
import subscriber from '../subscriber';

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

      grpcClientStreams.forEach((stream) => {
        if (
          stream.dataType === dataType
          && subscribersSessionIDs.includes(stream.sessionID)
        ) stream.write(JSON.parse(message));
      });
    } else if (keyNamespace === redis.SUBSINFO_SUBIDS && key === redis.GLOBALLYUNUSED) {
      const [dataFeedName] = redis.getFullKeyParts(room, 'dataFeedName');
      const theDataFeed = dataFeeds.getDataFeed(dataFeedName);

      const subToRemove = subscriber.subIDToSub(message);
      await theDataFeed.unsubscribe(subToRemove);

      const isGloballyRemoved = await redis.sremAsync(
        redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYUNUSED), message);
      logger.debug('isGloballyRemoved %j', isGloballyRemoved);
    }
  } catch (error) {
    logger.error('redisSub.on(message): %j', error);
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
    logger.debug('getPastMarketDataStream(): grpcCall from callID: %j', betterCallID);
    logger.debug('getPastMarketDataStream(): grpcCall request: %j', stream.request);

    const marketData = marketDatas.getMarketData(serviceName);
    const theDataFeed = marketData.getDataFeedBySubscription(stream.request);

    theDataFeed.getPastMarketDataStream(stream.request)
      .on('error', (error) => {
        logger.error('getPastMarketDataStream.on(error): callID: %j, %j', betterCallID, error);
        stream.emit('error', error);
      })
      .on('end', () => logger.debug('end of pastMarketData stream with callID: %j', betterCallID))
      .pipe(stream)
      .on('error', (error) => {
        logger.error('stream.on(error): callID: %j, %j', betterCallID, error);
      })
      ;
  } catch (error) {
    logger.error('getPastMarketDataStream(): callID: %j, %j', callID, error);
    throw error;
  }
}

async function getPastMarketDepthStream(stream) {
  try {
    stream.request.dataType = 'marketDepth';
    await getPastMarketDataStream(stream);
  } catch (error) {
    logger.error('getPastMarketDepthStream(): %j', error);
    stream.emit('error', error);
  }
}

async function getPastBarStream(stream) {
  try {
    stream.request.dataType = 'bar';
    await getPastMarketDataStream(stream);
  } catch (error) {
    logger.error('getPastBarStream(): %j', error);
    stream.emit('error', error);
  }
}

async function getPastTickerStream(stream) {
  try {
    stream.request.dataType = 'ticker';
    await getPastMarketDataStream(stream);
  } catch (error) {
    logger.error('getPastTickerStream(): %j', error);
    stream.emit('error', error);
  }
}

async function getPastDayBarStream(stream) {
  try {
    stream.request.dataType = 'dayBar';
    await getPastMarketDataStream(stream);
  } catch (error) {
    logger.error('getPastDayBarStream(): %j', error);
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
    logger.debug('subscribeMarketData() newSub: %j, from callID: %j', newSub, betterCallID);

    const marketData = marketDatas.getMarketData(serviceName);

    const theDataFeed = marketData.getDataFeedBySubscription(newSub);

    const subID = subscriber.subToSubID(theDataFeed.config.name, newSub);

    await subscriber.subscribeDataFeed(newSub, theDataFeed);
    await subscriber.subscribeRedis(subID);
    await subscriber.subscribeStream(subID, sessionID);

    callback(null, newSub);
  } catch (error) {
    logger.error('subscribeMarketData(): callID: %j, %j', callID, error);
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
    logger.debug('unsubscribeMarketData() subToRemove: %j, from callID: %j', subToRemove, betterCallID);

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
    logger.error('unsubscribeMarketData(): callID: %j, %j', callID, error);
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
    logger.debug('getMarketDataStream(): grpcCall from callID: %j', betterCallID);

    grpcClientStreams.forEach((existingStream) => {
      if (
        existingStream.sessionID === stream.sessionID
        && existingStream.dataType === stream.dataType
      ) throw new Error(`you already opened a similar stream of type "${stream.dataType}"`);
    });

    stream
      .on('cancelled', async () => {
        try {
          logger.error('stream.on(cancelled): callID: %j', betterCallID);
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
          logger.error('stream.on(cancelled): %j', error);
        }
      })
      .on('error', async (error) => {
        logger.error('stream.on(error): callID: %j, %j', betterCallID, error);
        try {
          grpcClientStreams.delete(stream);
          await redis.sremAsync(
            redis.join(redis.DATATYPE_SESSIONIDS, stream.dataType), stream.sessionID);
          await subscriber.removeSessionIDFromAllSubIDsByDataType(
            stream.sessionID, stream.dataType);
        } catch (err) {
          logger.error('stream.on(error) cleaning: %j', err);
        }
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
    logger.debug('existing subIDs that need to be subscribed: %j', needSubscribeSubIDs);

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
        logger.error('needSubscribeSubIDs.forEach(): %j', error);
      }
    });
  } catch (error) {
    logger.error('getMarketDataStream(): callID: %j, %j', callID, error);
    throw error;
  }
}

async function getMarketDepthStream(stream) {
  try {
    stream.dataType = 'marketDepth';
    await getMarketDataStream(stream);
  } catch (error) {
    logger.error('getMarketDepthStream(): %j', error);
    stream.emit('error', error);
  }
}

async function getBarStream(stream) {
  try {
    stream.dataType = 'bar';
    await getMarketDataStream(stream);
  } catch (error) {
    logger.error('getBarStream(): %j', error);
    stream.emit('error', error);
  }
}

async function getTickerStream(stream) {
  try {
    stream.dataType = 'ticker';
    await getMarketDataStream(stream);
  } catch (error) {
    logger.error('getTickerStream(): %j', error);
    stream.emit('error', error);
  }
}

async function getDayBarStream(stream) {
  try {
    stream.dataType = 'dayBar';
    await getMarketDataStream(stream);
  } catch (error) {
    logger.error('getDayBarStream(): %j', error);
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
    logger.debug('%j(): grpcCall from callID: %j', methodName, betterCallID);

    subs.forEach((sub) => {
      if (sub.dataType !== dataType) throw new Error(`cannot ask for dataType: "${sub.dataType}" with method: "${methodName}"`);
    });

    if (subs.length === 0) return callback(null, {});

    const marketData = marketDatas.getMarketData(serviceName);

    const subIDs = subs.map((sub) => {
      const dataFeedName = marketData.getDataFeedBySubscription(sub).config.name;
      return subscriber.subToSubID(dataFeedName, sub);
    });
    logger.debug('subIDs %j', subIDs);

    const lastRedisMarketDatas =
      await redis.mgetAsync(subIDs.map(subID => redis.join(redis.SUBID_LASTMD, subID)));
    const lastMarketDatas = lastRedisMarketDatas.filter(md => !!md).map(md => JSON.parse(md));
    logger.debug('lastMarketDatas.length %j', lastMarketDatas.length);

    const lastMarketDatasResponse = {};
    lastMarketDatasResponse[`${dataType}s`] = lastMarketDatas;

    callback(null, lastMarketDatasResponse);

    const globalSubIDs =
      await redis.smembersAsync(redis.join(redis.SUBSINFO_SUBIDS, redis.GLOBALLYSUBSCRIBED));
    const needSubscribeSubIDs = difference(subIDs, globalSubIDs);
    if (needSubscribeSubIDs.length > 0) logger.debug('existing subIDs that need to be subscribed: %j', needSubscribeSubIDs);

    needSubscribeSubIDs.forEach(async (newSubID) => {
      try {
        const newSub = subscriber.subIDToSub(newSubID);
        const [dataFeedName] = redis.getKeyParts(redis.SUBID, newSubID, 'dataFeedName');

        const theDataFeed = dataFeeds.getDataFeed(dataFeedName);

        await subscriber.subscribeDataFeed(newSub, theDataFeed);
      } catch (error) {
        logger.error('needSubscribeSubIDs.forEach(): %j', error);
      }
    });
  } catch (error) {
    logger.error('getLastMarketDatas(): callID: %j, %j', callID, error);
    callback(error);
  }
}

async function getLastMarketDepths(call, callback) {
  try {
    call.request.dataType = 'marketDepth';
    await getLastMarketDatas(call, callback);
  } catch (error) {
    logger.error('getLastMarketDepths(): %j', error);
    callback(error);
  }
}

async function getLastBars(call, callback) {
  try {
    call.request.dataType = 'bar';
    await getLastMarketDatas(call, callback);
  } catch (error) {
    logger.error('getLastBars(): %j', error);
    callback(error);
  }
}

async function getLastTickers(call, callback) {
  try {
    call.request.dataType = 'ticker';
    await getLastMarketDatas(call, callback);
  } catch (error) {
    logger.error('getLastTickers(): %j', error);
    callback(error);
  }
}

async function getLastDayBars(call, callback) {
  try {
    call.request.dataType = 'dayBar';
    await getLastMarketDatas(call, callback);
  } catch (error) {
    logger.error('getLastTickers(): %j', error);
    callback(error);
  }
}

async function getInstruments(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');
    const betterCallID = createBetterCallID(callID, user.userid);
    logger.debug('getInstruments() request: %j, grpcCall from callID: %j', call.request, betterCallID);

    const icePast = dataFeeds.getDataFeed('icePast');

    const dbInstruments = await icePast.getInstruments(call.request);

    // db instrumentname === null not compatible with proto3
    const instruments = dbInstruments.map((ins) => {
      if (ins.instrumentname === null) delete ins.instrumentname;
      delete ins._id;
      delete ins.updatedate;
      delete ins.update_date;
      return ins;
    });

    callback(null, { instruments });
  } catch (error) {
    logger.error('getInstruments(): callID: %j, %j', callID, error);
    callback(error);
  }
}

async function getMemoryInstruments(call, callback) {
  const callID = createCallID(call);
  try {
    logger.debug('token: %j', call.metadata.get('Authorization')[0]);
    logger.debug('sessionid: %j', call.metadata.get('sessionid')[0]);
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');
    const betterCallID = createBetterCallID(callID, user.userid);
    logger.debug('getMemoryInstruments() request: %j, grpcCall from callID: %j', call.request, betterCallID);

    const marketData = marketDatas.getMarketData(serviceName);
    const instruments = marketData.getMemoryInstruments(call.request);
    callback(null, { instruments });
  } catch (error) {
    logger.error('getMemoryInstruments(): callID: %j, %j', callID, error);
    callback(error);
  }
}

async function getSubscribableDataDescriptions(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');
    const betterCallID = createBetterCallID(callID, user.userid);

    logger.debug('getSubscribableDataDescriptions(): grpcCall from callID: %j', betterCallID);
    const marketData = marketDatas.getMarketData(serviceName);

    const subscribableDataDescriptions = await marketData.getSubscribableDataDescriptions();

    callback(null, { subscribableDataDescriptions });
  } catch (error) {
    logger.error('getSubscribableDataDescriptions(): callID: %j, %j', callID, error);
    callback(error);
  }
}

async function getMySubscriptions(call, callback) {
  const callID = createCallID(call);
  try {
    const user = await can.grpc(call, 'get', 'smartwinFuturesMd');
    const betterCallID = createBetterCallID(callID, user.userid);

    const sessionID = call.metadata.get('sessionid')[0];

    logger.debug('getMySubscriptions(): grpcCall from callID: %j', betterCallID);

    const subIDs = await subscriber.getSubIDsOfSessionID(sessionID);

    const subscriptions = subIDs.map(subID => subscriber.subIDToSub(subID));

    callback(null, { subscriptions });
  } catch (error) {
    logger.error('getMySubscriptions(): callID: %j, %j', callID, error);
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
  getMemoryInstruments,

  getSubscribableDataDescriptions,
  getMySubscriptions,
};

export default smartwinFutures;
