import createDebug from 'debug';

export default function createMarketDataStore(config) {
  const {
    dataFeed,
  } = config;
  const debug = createDebug(`marketDataStore ${dataFeed.config.name}`);

  try {
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
        debug('marketDataStore.length %o', marketDataStore.length);
      } catch (error) {
        debug('Error addMarketData(): %o', error);
      }
    };

    for (const dataType of dataFeed.config.dataTypes) {
      debug('registered event: dataType %o', dataType);
      dataFeed
        .on(dataType, (data) => {
          debug('add new MarketData %o', { symbol: data.symbol, resolution: data.resolution, dataType: data.dataType });
          addMarketData(data);
        })
        .on('error', error => debug('Error newDataFeed.onDataType: %o', error))
        ;
    }

    const getMarketDataStore = () => {
      try {
        return marketDataStore;
      } catch (error) {
        debug('Error getMarketDataStore(): %o', error);
      }
    };

    const getLastMarketData = (sub) => {
      try {
        const lastMarketData = marketDataStore.find(matchMarketData(sub));
        return lastMarketData;
      } catch (error) {
        debug('Error getLast(): %o', error);
      }
    };

    const marketDataStoreBase = {
      config,
      getMarketDataStore,
      getLastMarketData,
    };

    return marketDataStoreBase;
  } catch (error) {
    debug('Error createMarketDataStore(): %o', error);
  }
}
