import createDebug from 'debug';
import through from 'through2';
import createDataFeed from './dataFeed';

export default function createMarketData(config) {
  const {
    market,
    datafeed,
  } = config;

  const debug = createDebug(`marketData ${market}@${datafeed.name}`);

  try {
    const dataFeedsStore = [];
    const marketDataStore = [];
    const subscriptionsStore = [];

    const dataFeed = createDataFeed(config);

    const feedMarketDataStore = through.obj((data, enc, callback) => {
      try {
        const index = marketDataStore.findIndex(elem =>
          (elem.symbol === data.symbol && elem.resolution === data.resolution)
        );
        if (index === -1) {
          marketDataStore.push(data);
        } else {
          marketDataStore[index] = data;
        }
        callback(null, data);
      } catch (error) {
        debug('Error feedStore(): %o', error);
        callback(error);
      }
    });

    const getOrders = () => {
      const orders = ordersStore.map(elem => Object.assign({}, elem));
      return orders;
    };

    function getMarketData() {
      return marketDataStore;
    }

    function getSubscriptions() {
      return subscriptionsStore;
    }

    const feedStore = through.obj((data, enc, callback) => {
      try {
        const index = marketDataStore.findIndex(elem =>
          (elem.symbol === data.symbol && elem.resolution === data.resolution)
        );
        if (index === -1) {
          marketDataStore.push(data);
        } else {
          marketDataStore[index] = data;
        }
        callback(null, data);
      } catch (error) {
        debug('Error feedStore(): %o', error);
        callback(error);
      }
    });

    async function subscribe(newSub) {
      try {
        const {
          datafeed = 'iceLive',
          symbol,
          resolution,
        } = newSub;

        const similarSubs = subscriptionsStore
          .filter(sub => (sub.symbol === symbol && sub.resolution === resolution))
          ;

        if (similarSubs.length === 0) {
          const theDataFeed = dataFeedsStore.find(elem => elem.datafeed === datafeed);
          if (theDataFeed === undefined) throw new Error('no existing dataFeed');

          await theDataFeed.subscribe(symbol, resolution);
          subscriptionsStore.push(newSub);
        }
      } catch (error) {
        debug('Error subscribe(): %o', error);
      }
    }

    async function unsubscribe(newSub) {
      try {
        const {
          datafeed = 'iceLive',
          symbol,
          resolution,
        } = newSub;

        const similarSubIndex = subscriptionsStore
          .findIndex(sub => (sub.symbol === symbol && sub.resolution === resolution))
          ;

        if (similarSubIndex !== -1) {
          const theDataFeed = dataFeedsStore.find(elem => elem.datafeed === datafeed);
          if (theDataFeed === undefined) throw new Error('no dataFeed datafeed');

          await theDataFeed.unsubscribe(symbol, resolution);
          subscriptionsStore.splice(similarSubIndex, similarSubIndex + 1);
        }
      } catch (error) {
        debug('Error unsubscribe(): %o', error);
      }
    }

    const marketDataBase = {
      subscribe,
      unsubscribe,
      getMarketData,
      getSubscriptions,
    };
    const marketData = Object.assign(Object.create(dataFeed), marketDataBase);
    return marketData;
  } catch (error) {
    debug(`createMarketData() ${market}@${datafeed.name} Error: %o`, error);
  }
}
