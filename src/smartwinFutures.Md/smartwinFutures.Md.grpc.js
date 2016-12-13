import createDebug from 'debug';
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
  const fundid = call.request.fundid;
  const peer = call.getPeer();
  const streamID = `${sessionid.substr(0, 6)}:@${peer}>>@${fundid}`;
  return streamID;
}

async function setMarketDataStream(stream, eventName) {
  try {
    logError('the token: %o', stream.metadata.get('Authorization')[0]);
    const user = await grpcCan(stream, 'read', 'getOrders');

    const sessionid = stream.metadata.get('sessionid')[0];
    const streamDebug = createDebug(`${eventName}@${user.userid}:${sessionid.substr(0, 6)}@smartwinFuturesMd.grpc`);

    const marketData = marketDatas.getMarketData(serviceName);
    streamDebug('get%oStream()', eventName);

    const peer = stream.getPeer();
    streamDebug('peer %o', peer);

    const theDataFeed = marketData.getDataFeed(eventName);
    const theSessionSubs = subStores.addAndGetSubStore({ name: sessionid });

    const listener = (data) => {
      try {
        const isSubscribed = theSessionSubs.isSubscribed(data, theDataFeed.config.name);
        streamDebug('isSubscribed to %o: %o', `${data.symbol}:${data.dataType}`, isSubscribed);
        if (isSubscribed) {
          stream.write(data);
        }
      } catch (error) {
        streamDebug('listener() %o', error);
      }
    };

    theDataFeed
      .on(eventName, listener)
      .on('error', error => logError('theDataFeed.on(error): %o', eventName, error))
      ;

    stream
      .on('cancelled', () => {
        logError('connection cancelled');
        theDataFeed.removeListener(eventName, listener);
        theSessionSubs.removeDataTypeSubs(eventName, theDataFeed.config.name);
        dataFeeds.clearGlobalSubsDiff();
      })
      .on('error', (error) => {
        logError('stream.on(error): %o', eventName, error);
        theDataFeed.removeListener(eventName, listener);
      })
      ;
  } catch (error) {
    logError('setMarketDataStream() %o', error);
    stream.emit('error', error);
  }
}

async function getMarketDepthStream(stream) {
  try {
    const eventName = 'marketDepth';
    await setMarketDataStream(stream, eventName);
  } catch (error) {
    logError('getMarketDepthStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getBarStream(stream) {
  try {
    const eventName = 'bar';
    await setMarketDataStream(stream, eventName);
  } catch (error) {
    logError('getBarStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getTickerStream(stream) {
  try {
    const eventName = 'ticker';
    await setMarketDataStream(stream, eventName);
  } catch (error) {
    debug('getTickerStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getDayBarStream(stream) {
  try {
    const eventName = 'dayBar';
    await setMarketDataStream(stream, eventName);
  } catch (error) {
    logError('getDayBarStream(): %o', error);
    stream.emit('error', error);
  }
}

async function subscribeMarketData(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    const sessionid = call.metadata.get('sessionid')[0];
    const newSub = call.request;
    debug('subscribeMarketData() sub: %o', newSub);

    const marketData = marketDatas.getMarketData(serviceName);
    const subscription = await marketData.subscribeMarketData(sessionid, newSub);

    callback(null, subscription);
  } catch (error) {
    logError('subscribeMarketData(): %o', error);
    callback(error);
  }
}

async function unsubscribeMarketData(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    const sessionid = call.metadata.get('sessionid')[0];
    const subToRemove = call.request;
    debug('unsubscribeMarketData() subToRemove: %o', subToRemove);

    const marketData = marketDatas.getMarketData(serviceName);

    await marketData.unsubscribeMarketData(sessionid, subToRemove);

    callback(null, subToRemove);
  } catch (error) {
    logError('unsubscribeMarketData: %o', error);
    callback(error);
  }
}

async function getLastMarketDepths(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    const dataType = 'marketDepth';
    const subs = call.request.subscriptions.filter(sub => sub.dataType === dataType);
    debug('getLast%os: %o', dataType, subs);
    const sessionid = call.metadata.get('sessionid')[0];
    const marketData = marketDatas.getMarketData(serviceName);

    const marketDepths = marketData.getLastMarketDatas(sessionid, subs, dataType);
    debug('%os %o', dataType, marketDepths);
    callback(null, { marketDepths });
  } catch (error) {
    logError('getLastMarketDepths: %o', error);
    callback(error);
  }
}

async function getLastBars(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    const dataType = 'bar';
    const subs = call.request.subscriptions.filter(sub => sub.dataType === dataType);
    debug('getLast%os: %o', dataType, subs);
    const sessionid = call.metadata.get('sessionid')[0];
    const marketData = marketDatas.getMarketData(serviceName);

    const bars = marketData.getLastMarketDatas(sessionid, subs, dataType);
    debug('%os %o', dataType, bars);
    callback(null, { bars });
  } catch (error) {
    logError('getLastBars(): %o', error);
    callback(error);
  }
}

async function getLastTickers(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    const dataType = 'ticker';
    const subs = call.request.subscriptions.filter(sub => sub.dataType === dataType);
    debug('getLast%os: %o', dataType, subs);
    const sessionid = call.metadata.get('sessionid')[0];
    const marketData = marketDatas.getMarketData(serviceName);

    const tickers = marketData.getLastMarketDatas(sessionid, subs, dataType);

    debug('%os %o', dataType, tickers);
    callback(null, { tickers });
  } catch (error) {
    logError('getLastTickers(): %o', error);
    callback(error);
  }
}

async function getLastDayBars(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    const dataType = 'dayBar';
    const subs = call.request.subscriptions.filter(sub => sub.dataType === dataType);
    debug('getLast%os: %o', dataType, subs);
    const sessionid = call.metadata.get('sessionid')[0];
    const marketData = marketDatas.getMarketData(serviceName);

    const dayBars = marketData.getLastMarketDatas(sessionid, subs, dataType);

    debug('%os %o', dataType, dayBars);
    callback(null, { dayBars });
  } catch (error) {
    logError('getLastTickers(): %o', error);
    callback(error);
  }
}

async function getInstruments(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    debug('getInstruments() symbols: %o', call.request.symbols);
    const marketData = marketDatas.getMarketData(serviceName);

    const instruments = await marketData.getInstruments(call.request.symbols);
    // debug('instruments %o', instruments);
    callback(null, { instruments });
  } catch (error) {
    logError('getInstruments(): %o', error);
    callback(error);
  }
}

const smartwinFutures = {
  getMarketDepthStream,
  getBarStream,
  getTickerStream,
  getDayBarStream,
  subscribeMarketData,
  unsubscribeMarketData,
  getLastMarketDepths,
  getLastBars,
  getLastTickers,
  getLastDayBars,
  getInstruments,
};

export default smartwinFutures;
