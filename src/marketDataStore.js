import createDebug from 'debug';

const debug = createDebug('app:marketDataStore');
const logError = createDebug('app:marketDataStore:error');
logError.log = console.error.bind(console);

export default function createMarketDataStore(config) {
  try {
    const {
      dataFeed,
    } = config;

    const marketDataStore = [];

    const matchSubscription = newSub => sub => (
      sub.symbol === newSub.symbol &&
      sub.resolution === newSub.resolution &&
      sub.dataType === newSub.dataType);

    const matchMarketData = newSub => (sub) => {
      // debug('sub.symbol %o === newSub.symbol %o', sub.symbol, newSub.symbol);
      // debug('sub.resolution %o === newSub.resolution %o', sub.resolution, newSub.resolution);
      // debug('sub.dataType %o === newSub.dataType %o', sub.dataType, newSub.dataType);
      const isMatch = (
        sub.symbol === newSub.symbol &&
        sub.resolution === newSub.resolution &&
        sub.dataType === newSub.dataType);
      // debug('isMatch %o', isMatch);
      return isMatch;
    };


    const addMarketData = (data) => {
      try {
        const index = marketDataStore.findIndex(matchSubscription(data));
        if (index === -1) {
          marketDataStore.push(data);
        } else {
          marketDataStore[index] = data;
        }
        debug('added new MarketData %o', { symbol: data.symbol, resolution: data.resolution, dataType: data.dataType });
        debug('marketDataStore.length %o', marketDataStore.length);
      } catch (error) {
        logError('addMarketData(): %o', error);
        throw error;
      }
    };

    for (const dataDescription of dataFeed.config.dataDescriptions) {
      if (dataDescription.mode === 'live') {
        dataFeed
          .on(dataDescription.dataType, (data) => {
            addMarketData(data);
          })
          .on('error', error => logError('dataFeed.on(error): %o', error))
          ;
        debug('will store all following data description: %o', dataDescription);
      }
    }

    const getMarketDataStore = () => {
      try {
        return marketDataStore;
      } catch (error) {
        logError('getMarketDataStore(): %o', error);
        throw error;
      }
    };

    const getLastMarketData = (sub) => {
      try {
        const lastMarketData = marketDataStore.find(matchMarketData(sub));
        return lastMarketData;
      } catch (error) {
        logError('getLastMarketData(): %o', error);
        throw error;
      }
    };

    const marketDataStoreBase = {
      config,
      getMarketDataStore,
      getLastMarketData,
    };

    return marketDataStoreBase;
  } catch (error) {
    logError('createMarketDataStore(): %o', error);
    throw error;
  }
}
