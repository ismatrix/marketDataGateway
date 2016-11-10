import createDebug from 'debug';
import marketDatas from './marketDatas';
import subStores from './subscriptionStores';
import dataFeeds from './dataFeeds';

const debug = createDebug('smartwinFutures.grpc');

const marketDataName = 'smartwinFutures';

function setMarketDataStream(stream, eventName) {
  const sessionid = stream.metadata.get('sessionid')[0];
  const streamDebug = createDebug(`${eventName}@${sessionid}@smartwinFutures.grpc`);
  try {
    const marketData = marketDatas.getMarketData(marketDataName);
    streamDebug('get%oStream()', eventName);

    const peer = stream.getPeer();
    streamDebug('peer %o', peer);

    const theDataFeed = marketData.getDataFeed(eventName);
    const theSessionSubs = subStores.addAndGetSubStore({ name: sessionid });

    const listener = (data) => {
      const isSubscribed = theSessionSubs.isSubscribed(data, theDataFeed.config.name);
      streamDebug('isSubscribed', isSubscribed);
      if (isSubscribed) {
        stream.write(data);
      }
    };

    theDataFeed
      .on(eventName, listener)
      .on('error', error => streamDebug('%o.onError: %o', eventName, error))
      ;

    stream
      .on('cancelled', () => {
        streamDebug('cancelled connection');
        theDataFeed.removeListener(eventName, listener);
        theSessionSubs.removeDataTypeSubs(eventName, theDataFeed.config.name);
        dataFeeds.clearGlobalSubsDiff();
      })
      .on('error', error => streamDebug('%oStream.onError: %o', eventName, error))
      ;
  } catch (error) {
    streamDebug('Error setMarketDataStream() %o', error);
  }
}

async function getMarketDepthStream(stream) {
  try {
    const eventName = 'marketDepth';
    setMarketDataStream(stream, eventName);
  } catch (error) {
    debug('Error getMarketDepthStream(): %o', error);
  }
}

async function getBarStream(stream) {
  try {
    const eventName = 'bar';
    setMarketDataStream(stream, eventName);
  } catch (error) {
    debug('Error getBarStream(): %o', error);
  }
}

async function getTickerStream(stream) {
  try {
    const eventName = 'ticker';
    setMarketDataStream(stream, eventName);
  } catch (error) {
    debug('Error getTickerStream(): %o', error);
  }
}

async function getDayBarStream(stream) {
  try {
    const eventName = 'dayBar';
    setMarketDataStream(stream, eventName);
  } catch (error) {
    debug('Error getDayBarStream(): %o', error);
  }
}

async function subscribeMarketData(call, callback) {
  try {
    const sessionid = call.metadata.get('sessionid')[0];
    const newSub = call.request;
    debug('subscribeMarketData() sub: %o', newSub);

    const marketData = marketDatas.getMarketData(marketDataName);
    const subscription = await marketData.subscribeMarketData(sessionid, newSub);

    callback(null, subscription);
  } catch (error) {
    debug('Error subscribeMarketData %o', error);
    callback(error);
  }
}

async function unsubscribeMarketData(call, callback) {
  try {
    const sessionid = call.metadata.get('sessionid')[0];
    const subToRemove = call.request;
    debug('unsubscribeMarketData() subToRemove: %o', subToRemove);

    const marketData = marketDatas.getMarketData(marketDataName);

    await marketData.unsubscribeMarketData(sessionid, subToRemove);

    callback(null, subToRemove);
  } catch (error) {
    debug('Error unsubscribeMarketData %o', error);
    callback(error);
  }
}

async function getLastMarketDepths(call, callback) {
  try {
    const dataType = 'marketDepth';
    const subs = call.request.subscriptions.filter(sub => sub.dataType === dataType);
    debug('getLast%os: %o', dataType, subs);
    const sessionid = call.metadata.get('sessionid')[0];
    const marketData = marketDatas.getMarketData(marketDataName);

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
    const dataType = 'bar';
    const subs = call.request.subscriptions.filter(sub => sub.dataType === dataType);
    debug('getLast%os: %o', dataType, subs);
    const sessionid = call.metadata.get('sessionid')[0];
    const marketData = marketDatas.getMarketData(marketDataName);

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
    const dataType = 'ticker';
    const subs = call.request.subscriptions.filter(sub => sub.dataType === dataType);
    debug('getLast%os: %o', dataType, subs);
    const sessionid = call.metadata.get('sessionid')[0];
    const marketData = marketDatas.getMarketData(marketDataName);

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
    const dataType = 'dayBar';
    const subs = call.request.subscriptions.filter(sub => sub.dataType === dataType);
    debug('getLast%os: %o', dataType, subs);
    const sessionid = call.metadata.get('sessionid')[0];
    const marketData = marketDatas.getMarketData(marketDataName);

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
    debug('symbols: %o', call.request.symbols);
    const marketData = marketDatas.getMarketData(marketDataName);

    const instruments = await marketData.getInstruments(call.request.symbols);
    debug('instruments %o', instruments);
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
