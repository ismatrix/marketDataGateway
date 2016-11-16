import createDebug from 'debug';
import marketDatas from './marketDatas';
import subStores from './subscriptionStores';
import dataFeeds from './dataFeeds';
import grpcCan from './acl';

const debug = createDebug('smartwinFuturesMd.grpc');

const serviceName = 'smartwinFuturesMd';

async function setMarketDataStream(stream, eventName) {
  const user = await grpcCan(stream, 'read', 'getOrders');

  const sessionid = stream.metadata.get('sessionid')[0];
  const streamDebug = createDebug(`${eventName}@${user.userid}:${sessionid.substr(0, 6)}@smartwinFuturesMd.grpc`);

  try {
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
        streamDebug('Error listener() %o', error);
      }
    };

    theDataFeed
      .on(eventName, listener)
      .on('error', error => streamDebug('%o.onError: %o', eventName, error))
      ;

    stream
      .on('cancelled', () => {
        streamDebug('connection cancelled');
        theDataFeed.removeListener(eventName, listener);
        theSessionSubs.removeDataTypeSubs(eventName, theDataFeed.config.name);
        dataFeeds.clearGlobalSubsDiff();
      })
      .on('error', (error) => {
        streamDebug('%oStream.onError: %o', eventName, error);
        theDataFeed.removeListener(eventName, listener);
      })
      ;
  } catch (error) {
    streamDebug('Error setMarketDataStream() %o', error);
    stream.emit('error', error);
  }
}

async function getMarketDepthStream(stream) {
  try {
    const eventName = 'marketDepth';
    await setMarketDataStream(stream, eventName);
  } catch (error) {
    debug('Error getMarketDepthStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getBarStream(stream) {
  try {
    const eventName = 'bar';
    await setMarketDataStream(stream, eventName);
  } catch (error) {
    debug('Error getBarStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getTickerStream(stream) {
  try {
    const eventName = 'ticker';
    await setMarketDataStream(stream, eventName);
  } catch (error) {
    debug('Error getTickerStream(): %o', error);
    stream.emit('error', error);
  }
}

async function getDayBarStream(stream) {
  try {
    const eventName = 'dayBar';
    await setMarketDataStream(stream, eventName);
  } catch (error) {
    debug('Error getDayBarStream(): %o', error);
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
    debug('Error subscribeMarketData %o', error);
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
    debug('Error unsubscribeMarketData %o', error);
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
    debug('Error getLastMarketDepths %o', error);
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
    debug('Error getLastBars %o', error);
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
    debug('Error getLastTickers %o', error);
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
    debug('Error getLastTickers %o', error);
    callback(error);
  }
}

async function getInstruments(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    debug('symbols: %o', call.request.symbols);
    const marketData = marketDatas.getMarketData(serviceName);

    const instruments = await marketData.getInstruments(call.request.symbols);
    // debug('instruments %o', instruments);
    callback(null, { instruments });
  } catch (error) {
    debug('Error getInstruments %o', error);
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
