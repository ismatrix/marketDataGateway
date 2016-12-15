import createDebug from 'debug';
import { upperFirst } from 'lodash';
import marketDatas from '../marketDatas';
import subStores from '../subscriptionStores';
import dataFeeds from '../dataFeeds';
import grpcCan from '../acl';

const debug = createDebug('app:smartwinFuturesMd.grpc');
const logError = createDebug('app:smartwinFuturesMd.grpc:error');
logError.log = console.error.bind(console);

const serviceName = 'smartwinFuturesMd';

function createCallID(call) {
  const sessionid = call.metadata.get('sessionid')[0];
  const peer = call.getPeer();
  const callID = `${peer}@${sessionid.substr(0, 6)}`;
  return callID;
}

function createBetterCallID(callID, ...args) {
  return [callID, ...args].join('@');
}

async function getMarketDataStream(stream, eventName) {
  const callID = createCallID(stream);
  try {
    const user = await grpcCan(stream, 'read', 'getOrders');
    const sessionid = stream.metadata.get('sessionid')[0];

    const betterCallID = createBetterCallID(callID, user.userid, eventName);
    debug('getMarketDataStream(): grpcCall from callID: %o', betterCallID);

    const marketData = marketDatas.getMarketData(serviceName);

    const theDataFeed = marketData.getDataFeedByDataType(eventName);
    const theSessionSubs = subStores.addAndGetSubStore({ name: sessionid });

    const listener = (data) => {
      try {
        const isSubscribed = theSessionSubs.isSubscribed(data, theDataFeed.config.name);
        debug('callID: %o, isSubscribed to %o: %o', betterCallID, `${data.symbol}:${data.dataType}`, isSubscribed);
        if (isSubscribed) {
          stream.write(data);
        }
      } catch (error) {
        logError('listener(): callID: %o, %o', betterCallID, error);
      }
    };

    theDataFeed
      .on(eventName, listener)
      .on('error', error => logError('theDataFeed.on(error): callID: %o, %o', betterCallID, error))
      ;

    stream
      .on('cancelled', () => {
        logError('stream.on(cancelled): callID: %o', betterCallID);
        theDataFeed.removeListener(eventName, listener);
        theSessionSubs.removeDataTypeSubs(eventName, theDataFeed.config.name);
        dataFeeds.clearGlobalSubsDiff();
      })
      .on('error', (error) => {
        logError('stream.on(error): callID: %o, %o', betterCallID, error);
        theDataFeed.removeListener(eventName, listener);
      })
      ;
  } catch (error) {
    logError('getMarketDataStream(): callID: %o, %o', callID, error);
    throw error;
  }
}

async function getMarketDepthStream(stream) {
  try {
    const eventName = 'marketDepth';
    await getMarketDataStream(stream, eventName);
  } catch (error) {
    logError('getMarketDepthStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getBarStream(stream) {
  try {
    const eventName = 'bar';
    await getMarketDataStream(stream, eventName);
  } catch (error) {
    logError('getBarStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getTickerStream(stream) {
  try {
    const eventName = 'ticker';
    await getMarketDataStream(stream, eventName);
  } catch (error) {
    debug('getTickerStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getDayBarStream(stream) {
  try {
    const eventName = 'dayBar';
    await getMarketDataStream(stream, eventName);
  } catch (error) {
    logError('getDayBarStream(): %o', error);
    stream.emit('error', error);
  }
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
    const subscription = await marketData.subscribeMarketData(sessionid, newSub);

    callback(null, subscription);
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

    await marketData.unsubscribeMarketData(sessionid, subToRemove);

    callback(null, subToRemove);
  } catch (error) {
    logError('unsubscribeMarketData(): callID: %o, %o', callID, error);
    callback(error);
  }
}

async function getLastMarketDatas(call, callback) {
  const callID = createCallID(call);
  try {
    const dataType = call.request.dataType;
    const methodName = `getLast${upperFirst(dataType)}s`;
    const user = await grpcCan(call, 'read', 'getOrders');
    const betterCallID = createBetterCallID(callID, user.userid);

    const subs = call.request.subscriptions.filter(sub => sub.dataType === dataType);

    const subsSymbols = subs.map(sub => sub.symbol);
    debug('%o(): %o, grpcCall from callID: %o', methodName, subsSymbols, betterCallID);
    const sessionid = call.metadata.get('sessionid')[0];
    const marketData = marketDatas.getMarketData(serviceName);

    const lastMarketDatas = {};
    lastMarketDatas[`${dataType}s`] = marketData.getLastMarketDatas(sessionid, subs, dataType);

    callback(null, lastMarketDatas);
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
};

export default smartwinFutures;
