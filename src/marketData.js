import createDebug from 'debug';
import through from 'through2';
import { differenceWith, isEqual } from 'lodash';
import createDataFeed from './dataFeed';
import mongodb from './mongodb';

export default function createMarketData(config) {
  const {
    name,
  } = config;

  const debug = createDebug(`marketData ${name}@${config.dataFeed.name}`);
  const smartwinDB = mongodb.getdb();

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
      debug('data: %o', data);
      addMarketData(data);
      // callback(null, data);
      callback();
    });

    const dataFeed = createDataFeed(config);

    dataFeed.getDataFeed()
      .pipe(feedMarketDataStore)
      .on('error', error => debug('Error dataFeed.pipe(feedMarketDataStore): %o', error))
      .on('end', () => debug('END event of through marketDataStore'))
      ;

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
      sub.symbol === newSub.symbol &&
      sub.resolution === newSub.resolution &&
      sub.dataType === newSub.dataType);

    const matchMarketData = newSub => (sub) => {
      debug('sub.symbol %o === newSub.symbol %o', sub.symbol, newSub.symbol);
      debug('sub.resolution %o === newSub.resolution %o', sub.resolution, newSub.resolution);
      debug('sub.dataType %o === newSub.dataType %o', sub.dataType, newSub.dataType);
      const isMatch = (
        sub.symbol === newSub.symbol &&
        sub.resolution === newSub.resolution &&
        sub.dataType === newSub.dataType);
      debug('isMatch %o', isMatch);
      return isMatch;
    };

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
        return newSub;
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
          const removedSub = subscriptionsStore.splice(similarSubIndex, similarSubIndex + 1);
          debug('removedSub %o', removedSub);
          return removedSub;
        }
        return newSub;
      } catch (error) {
        debug('Error unsubscribe(): %o', error);
      }
    };

    const updateSubscriptions = async (subs) => {
      try {
        const globalSubs = getSubscriptions();

        const needSubscribeSubs = differenceWith(subs, globalSubs, isEqual);
        debug('updateSubscriptions() needSubscribe: %o', needSubscribeSubs);
        needSubscribeSubs.map(sub => subscribe(sub));

        const needUnsubscribeSubs = differenceWith(globalSubs, subs, isEqual);
        debug('updateSubscriptions() needUnsubscribe: %o', needUnsubscribeSubs);
        needUnsubscribeSubs.map(sub => unsubscribe(sub));
      } catch (error) {
        debug('Error unsubscribe(): %o', error);
      }
    };

    const getLastMarketData = (sub) => {
      try {
        debug('getLastMarketData() sub %o', sub);
        debug('getLastMarketData() marketDataStore %o', marketDataStore);
        const lastMarketData = marketDataStore
          .find(matchMarketData(sub))
          ;
        debug('lastMarketData %o', lastMarketData);
        return lastMarketData;
      } catch (error) {
        debug('Error getLast(): %o', error);
      }
    };

    const getInstruments = async (symbols) => {
      try {
        const INSTRUMENT = smartwinDB.collection('INSTRUMENT');
        const query = { instrumentid: { $in: symbols } };
        const projection = { _id: 0 };
        const instruments = await INSTRUMENT.find(query, projection).toArray();
        debug('instruments %o', instruments);

        instruments.map((ins) => {
          ins.updatedate = ins.updatedate.toISOString();
          if ('update_date' in ins) delete ins.update_date;
          return ins;
        });
        return instruments;
      } catch (error) {
        debug('Error getInstruments(): %o', error);
      }
    };

    const marketDataBase = {
      subscribe,
      unsubscribe,
      updateSubscriptions,
      getMarketData,
      getLastMarketData,
      getSubscriptions,
      getInstruments,
    };
    const marketData = Object.assign(Object.create(dataFeed), marketDataBase);
    return marketData;
  } catch (error) {
    debug(`createMarketData() ${name}@${config.dataFeed.name} Error: %o`, error);
  }
}
