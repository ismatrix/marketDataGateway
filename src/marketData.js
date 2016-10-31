import createDebug from 'debug';
import through from 'through2';
import createDataFeed from './dataFeed';

export default function createMarketData(config) {
  const {
    name,
  } = config;

  const debug = createDebug(`marketData ${name}@${config.dataFeed.name}`);

  try {
    const marketDataStore = [];
    const subscriptionsStore = [];

    const addMarketData = (data) => {
      try {
        const index = marketDataStore.findIndex(elem =>
          (
            elem.symbol === data.symbol &&
            elem.resolution === data.resolution &&
            elem.dataType === data.dataType
          )
        );
        if (index === -1) {
          marketDataStore.push(data);
        } else {
          marketDataStore[index] = data;
        }
      } catch (error) {
        debug('Error addMarketData(): %o', error);
      }
    };

    const feedMarketDataStore = through.obj((data, enc, callback) => {
      try {
        addMarketData(data);
        callback(null, data);
      } catch (error) {
        debug('Error feedStore(): %o', error);
        callback(error);
      }
    });

    const dataFeed = createDataFeed(config);
    dataFeed.getDataFeed().pipe(feedMarketDataStore);

    const getMarketData = () => {
      try {
        return marketDataStore;
      } catch (error) {
        debug('Error getMarketData(): %o', error);
      }
    };

    const getSubscriptions = () => {
      try {
        return subscriptionsStore;
      } catch (error) {
        debug('Error getSubscriptions(): %o', error);
      }
    };

    const matchSubscription = newSub => sub => (
      sub.instrument.symbol === newSub.instrument.symbol &&
      sub.resolution === newSub.resolution &&
      sub.dataType === newSub.dataType);

    const matchMarketData = newSub => sub => (
      sub.symbol === newSub.instrument.symbol &&
      sub.resolution === newSub.resolution &&
      sub.dataType === newSub.dataType);

    const subscribe = async (newSub) => {
      try {
        const similarSub = subscriptionsStore
          .find(matchSubscription(newSub))
          ;

        if (!similarSub) {
          await dataFeed.subscribe(newSub);
          subscriptionsStore.push(newSub);
          return newSub;
        }
      } catch (error) {
        debug('Error subscribe(): %o', error);
      }
    };

    const unsubscribe = async (newSub) => {
      try {
        const similarSubIndex = subscriptionsStore
          .findIndex(matchSubscription(newSub))
          ;

        if (similarSubIndex !== -1) {
          await dataFeed.unsubscribe(newSub);
          const removedSubs = subscriptionsStore.splice(similarSubIndex, similarSubIndex + 1);
          debug('removedSubs %o', removedSubs);
        }
      } catch (error) {
        debug('Error unsubscribe(): %o', error);
      }
    };

    const getLastMarketData = (sub) => {
      try {
        const lastMarketData = marketDataStore
          .find(matchMarketData(sub))
          ;
        return lastMarketData;
      } catch (error) {
        debug('Error getLast(): %o', error);
      }
    };

    const marketDataBase = {
      subscribe,
      unsubscribe,
      getMarketData,
      getLastMarketData,
      getSubscriptions,
    };
    const marketData = Object.assign(Object.create(dataFeed), marketDataBase);
    return marketData;
  } catch (error) {
    debug(`createMarketData() ${name}@${config.dataFeed.name} Error: %o`, error);
  }
}
