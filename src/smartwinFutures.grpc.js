import createDebug from 'debug';
import marketDatas from './marketDatas';
import subStores from './subscriptionStores';
import dataFeeds from './dataFeeds';

const debug = createDebug('smartwinFutures.grpc');

const marketDataName = 'smartwinFutures';

async function getMarketDepthStream(stream) {
  try {
    const marketData = marketDatas.getMarketData(marketDataName);

    const sessionid = stream.metadata.get('sessionid')[0];
    debug('getMarketDepthStream() sessionid %o', sessionid);

    const theDataFeed = marketData.getDataFeed('marketDepth');
    const theSessionSubs = subStores.addAndGetSubStore({ name: sessionid });

    theDataFeed
      .on('marketDepth', (data) => {
        const isSubscribed = theSessionSubs.isSubscribed(data);

        if (isSubscribed) {
          stream.write(data);
        }
      })
      .on('error', error => debug('getMarketDepthStream.onError: %o', error))
      ;

    stream.on('cancelled', () => {
      debug('cancelled connection for %o', sessionid);
      theSessionSubs.removeDataTypeSubs('marketDepth');
      const allSubs = sessionSubs.getAll();
      marketData.updateSubscriptions(allSubs);
    });
  } catch (error) {
    debug('Error getMarketDepthStream(): %o', error);
  }
}

async function getBarStream(stream) {
  try {
    const marketData = marketDatas.getMarketData(marketDataName);

    const sessionid = stream.metadata.get('sessionid')[0];
    debug('getBarStream() sessionid %o', sessionid);

    const theDataFeed = marketData.getDataFeed('bar');
    theDataFeed
      .on('bar', (data) => {
        const theSessionSubs = sessionSubs.getSessionSubs(sessionid);

        const similarSubIndex = theSessionSubs
          .findIndex(matchSubscription(data))
          ;

        if (similarSubIndex !== -1) {
          stream.write(data);
        }
      })
      .on('error', error => debug('getBarStream.onError: %o', error))
      ;

    stream.on('cancelled', () => {
      debug('cancelled connection for %o', sessionid);
      sessionSubs.removeDataTypeSubs(sessionid, 'bar');
      const allSubs = sessionSubs.getAll();
      marketData.updateSubscriptions(allSubs);
    });
  } catch (error) {
    debug('Error getBarStream(): %o', error);
  }
}

async function getTickerStream(stream) {
  try {
    const marketData = marketDatas.getMarketData(marketDataName);

    const sessionid = stream.metadata.get('sessionid')[0];
    debug('getTickerStream() sessionid %o', sessionid);

    const peer = stream.getPeer();
    debug('peer %o', peer);

    const theDataFeed = marketData.getDataFeed('ticker');
    theDataFeed
      .on('ticker', (data) => {
        const theSessionSubs = sessionSubs.getSessionSubs(sessionid);

        const similarSubIndex = theSessionSubs
          .findIndex(matchSubscription(data))
          ;

        if (similarSubIndex !== -1) {
          stream.write(data);
        }
      })
      .on('error', error => debug('getTickerStream.onError: %o', error))
      ;

    stream.on('cancelled', () => {
      debug('cancelled connection for %o', sessionid);
      sessionSubs.removeDataTypeSubs(sessionid, 'ticker');
      const allSubs = sessionSubs.getAll();
      marketData.updateSubscriptions(allSubs);
    });
  } catch (error) {
    debug('Error getTickerStream(): %o', error);
  }
}

async function subscribeMarketData(call, callback) {
  try {
    const sessionid = call.metadata.get('sessionid')[0];
    const newSub = call.request;

    const marketData = marketDatas.getMarketData(marketDataName);
    const theDataFeed = marketData.getDataFeed(newSub.dataType);

    newSub.dataFeedName = theDataFeed.config.name;
    debug('subscribeMarketData() sub: %o', newSub);

    const subscription = await dataFeeds.subscribe(theDataFeed, newSub);
    debug('subscribeResult %o', subscription);

    const theSessionSubs = subStores.addAndGetSubStore({ name: sessionid });
    theSessionSubs.addSub(newSub);

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

    const theSessionSubs = subStores.addAndGetSubStore({ name: sessionid });
    theSessionSubs.removeSub(subToRemove);

    callback(null, subToRemove);

    const marketData = marketDatas.getMarketData(marketDataName);
    const theDataFeed = marketData.getDataFeed(subToRemove.dataType);
    dataFeeds.unsubscribe(theDataFeed, subToRemove);
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
