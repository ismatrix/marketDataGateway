import createDebug from 'debug';
import marketDatas from './marketDatas';
import sessionSubs from './sessionSubscriptions';

const debug = createDebug('smartwinFutures.grpc');

const marketDataName = 'smartwinFutures';

async function getMarketDepthStream(stream) {
  try {
    debug('getMarketDepthStream() stream: %o', stream);
    const marketData = marketDatas.getMarketData(marketDataName);
    marketData.getDataFeed()
      .on('data', (data) => {
        if (data.dataType === 'marketDepth') {
          stream.write(data);
        }
      })
      ;
  } catch (error) {
    debug('Error getMarketDepthStream(): %o', error);
  }
}

async function getBarStream(stream) {
  try {
    const marketData = marketDatas.getMarketData(marketDataName);
    marketData.getDataFeed()
      .on('data', (data) => {
        if (data.dataType === 'bar') {
          stream.write(data);
        }
      })
      ;
  } catch (error) {
    debug('Error getBarStream(): %o', error);
  }
}

async function getTickerStream(stream) {
  try {
    debug('getTickerStream() stream: %o', stream);
    const marketData = marketDatas.getMarketData(marketDataName);

    const sessionid = stream.metadata.get('sessionid')[0];
    debug('sessionid %o', sessionid);

    const peer = stream.getPeer();
    debug('peer %o', peer);

    stream.on('cancelled', () => {
      debug('cancelled connection for %o', sessionid);
      sessionSubs.removeSession(sessionid);
      const allSubs = sessionSubs.getAll();
      marketData.updateSubscriptions(allSubs);
    });

    marketData.getDataFeed()
      .on('data', (data) => {
        if (data.dataType === 'ticker') {
          stream.write(data);
        }
      })
      .on('error', error => debug('dataFeed.onData Error: %o', error))
      ;
  } catch (error) {
    debug('Error getTickerStream(): %o', error);
  }
}

async function subscribeMarketData(call, callback) {
  try {
    const sessionid = call.metadata.get('sessionid')[0];
    const newSub = call.request;
    debug('subscribeMarketData() sub: %o', newSub);

    const marketData = marketDatas.getMarketData(marketDataName);

    const subscription = await marketData.subscribe(newSub);
    debug('subscribeResult %o', subscription);

    sessionSubs.add(sessionid, newSub);
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
    debug('unsubscribeMarketData() sub: %o', subToRemove);

    sessionSubs.removeSub(sessionid, subToRemove);
    callback(null, subToRemove);

    const marketData = marketDatas.getMarketData(marketDataName);
    const allSubs = sessionSubs.getAll();
    marketData.updateSubscriptions(allSubs);
  } catch (error) {
    debug('Error unsubscribeMarketData %o', error);
    callback(error);
  }
}

async function getLastMarketDepths(call, callback) {
  try {
    debug('subscriptions: %o', call.request.subscriptions);
    const marketData = marketDatas.getMarketData(marketDataName);
    const subsPromise = call.request.subscriptions.map(sub => marketData.subscribe(sub));
    const subsPromiseResult = await Promise.all(subsPromise.map(p => p.catch(e => e)));
    debug('subsPromiseResult %o', subsPromiseResult);

    const marketDepths = call.request.subscriptions
      .map(sub => marketData.getLastMarketData(sub))
      .filter(md => (!!md && !!md.dataType && md.dataType === 'marketDepth'))
      ;
    debug('marketDepths %o', marketDepths);
    callback(null, { marketDepths });
  } catch (error) {
    debug('Error getLastMarketDepths %o', error);
    callback(error);
  }
}

async function getLastBars(call, callback) {
  try {
    debug('subscriptions: %o', call.request.subscriptions);
    const marketData = marketDatas.getMarketData(marketDataName);
    const subsPromise = call.request.subscriptions.map(sub => marketData.subscribe(sub));
    const subsPromiseResult = await Promise.all(subsPromise.map(p => p.catch(e => e)));
    debug('subsPromiseResult %o', subsPromiseResult);

    const bars = call.request.subscriptions
      .map(sub => marketData.getLastMarketData(sub))
      .filter(md => (!!md && !!md.dataType && md.dataType === 'bar'))
      ;
    debug('bars %o', bars);
    callback(null, { bars });
  } catch (error) {
    debug('Error getLastBars %o', error);
    callback(error);
  }
}

async function getLastTickers(call, callback) {
  try {
    debug('subscriptions: %o', call.request.subscriptions);
    const marketData = marketDatas.getMarketData(marketDataName);

    const subsPromise = call.request.subscriptions.map(sub => marketData.subscribe(sub));
    const subsPromiseResult = await Promise.all(subsPromise.map(p => p.catch(e => e)));
    debug('subsPromiseResult %o', subsPromiseResult);

    const tickers = call.request.subscriptions
      .map(sub => marketData.getLastMarketData(sub))
      .filter(md => (!!md && !!md.dataType && md.dataType === 'ticker'))
      ;
    debug('tickers %o', tickers);
    callback(null, { tickers });
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
  subscribeMarketData,
  unsubscribeMarketData,
  getLastMarketDepths,
  getLastBars,
  getLastTickers,

  getInstruments,
};

export default smartwinFutures;
