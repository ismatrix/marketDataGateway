import createDebug from 'debug';
import marketDatas from './marketDatas';

const debug = createDebug('smartwinFutures.grpc');

const marketDataName = 'smartwinFutures';

async function streamMarketDepth(stream) {
  try {
    const marketData = marketDatas.getMarketData(marketDataName);

    stream.on('data', async (data) => {
      marketData.subscribeMarketDepth(data);
    });
    marketData.getDataFeed().pipe(stream);
  } catch (error) {
    debug('Error SubscribeMarketDepth(): %o', error);
  }
}

async function streamBar(stream) {
  try {
    const marketData = marketDatas.getMarketData(marketDataName);

    stream.on('data', async (data) => {
      marketData.subscribeMarketDepth(data);
    });
    marketData.getDataFeed().pipe(stream);
  } catch (error) {
    debug('Error SubscribeBar(): %o', error);
  }
}

async function getLastMarketDepths(call, callback) {
  try {
    debug('subscriptions: %o', call.request.subscriptions);
    const marketData = marketDatas.getMarketData(marketDataName);
    const subsPromise = call.request.subscriptions.map(sub => marketData.subscribe(sub));
    const subsPromiseResult = await Promise.all(subsPromise);
    debug('subsPromiseResult %o', subsPromiseResult);
    const marketDepths = call.request.subscriptions
      .map(sub => marketData.getLastMarketData(sub))
      .filter(md => (!!md && !!md.dataType && md.dataType !== 'marketDepth'))
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
    const subsPromiseResult = await Promise.all(subsPromise);
    debug('subsPromiseResult %o', subsPromiseResult);
    const bars = call.request.subscriptions
      .map(sub => marketData.getLastMarketData(sub))
      .filter(md => (!!md && !!md.dataType && md.dataType !== 'bar'))
      ;
    debug('bars %o', bars);
    callback(null, { bars });
  } catch (error) {
    debug('Error getLastBars %o', error);
    callback(error);
  }
}

const smartwinFutures = {
  streamMarketDepth,
  streamBar,
  getLastMarketDepths,
  getLastBars,
};

export default smartwinFutures;
