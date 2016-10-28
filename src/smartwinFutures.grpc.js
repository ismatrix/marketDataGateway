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
    const marketData = marketDatas.getMarketData(marketDataName);
    const marketDepths = marketData.getLastMarketDepths(call.request);
    callback(null, { marketDepths });
  } catch (error) {
    debug('Error getLastMarketDepths %o', error);
    callback(error);
  }
}

async function getLastBars(call, callback) {
  try {
    const marketData = marketDatas.getMarketData(marketDataName);
    // const bars = marketData.getLastBars(call.request);
    const bars = [];
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
