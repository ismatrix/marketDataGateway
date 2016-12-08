import createDebug from 'debug';
import mongodb from './mongodb';
import dataFeeds from './dataFeeds';
import subStores from './subscriptionStores';
import mdStores from './marketDataStores';

const debug = createDebug('app:marketData');
const logError = createDebug('app:marketData:error');
logError.log = console.error.bind(console);


export default function createMarketData(config) {
  try {
    const smartwinDB = mongodb.getdb();

    for (const dataFeedConfig of config.dataFeeds) {
      dataFeeds.addDataFeed(dataFeedConfig);
    }

    const init = async () => {
      try {
        const connectPromises = config.dataFeeds
          .map(dataFeedConfig => dataFeeds.getDataFeed(dataFeedConfig.name))
          .map(dataFeed => dataFeed.connect())
          ;
        debug('connectPromises %o', connectPromises);
        await Promise.all(connectPromises);
      } catch (error) {
        logError('init(): %o', error);
        throw error;
      }
    };

    const dataTypeToDataFeedName = (dataType) => {
      try {
        for (const dataFeed of config.dataFeeds) {
          if (dataFeed.dataTypes.includes(dataType)) return dataFeed.name;
        }
        throw new Error('dataType not found in dataFeeds config');
      } catch (error) {
        logError('dataTypeToDataFeedName(): %o', error);
        throw error;
      }
    };

    const subscribeMarketData = async (sessionid, newSub) => {
      try {
        const theDataFeedName = dataTypeToDataFeedName(newSub.dataType);
        const subscription = await dataFeeds.subscribe(theDataFeedName, newSub);
        // debug('global subscribeResult %o', subscription);

        const theSessionSubs = subStores.addAndGetSubStore({ name: sessionid });
        theSessionSubs.addSub(newSub, theDataFeedName);

        return subscription;
      } catch (error) {
        logError('subscribeMarketData(): %o', error);
        throw error;
      }
    };

    const unsubscribeMarketData = async (sessionid, subToRemove) => {
      try {
        const theDataFeedName = dataTypeToDataFeedName(subToRemove.dataType);

        const theSessionSubs = subStores.addAndGetSubStore({ name: sessionid });
        theSessionSubs.removeSub(subToRemove, theDataFeedName);

        dataFeeds.unsubscribe(theDataFeedName, subToRemove);
      } catch (error) {
        logError('unsubscribeMarketData(): %o', error);
        throw error;
      }
    };

    const getDataFeed = (dataType) => {
      try {
        const dataFeedConf = config.dataFeeds.find(
          dfConfig => dfConfig.dataTypes.includes(dataType));
        if ('name' in dataFeedConf) {
          const theDataFeed = dataFeeds.getDataFeed(dataFeedConf.name);
          return theDataFeed;
        }
        throw new Error('No dataFeed for this dataType');
      } catch (error) {
        logError('getDataFeed(): %o', error);
        throw error;
      }
    };

    const getLastMarketDatas = (sessionid, subs, dataType) => {
      try {
        subs.map(sub => subscribeMarketData(sessionid, sub));

        const mdStore = mdStores.getMdStoreByName(dataTypeToDataFeedName(dataType));
        const tickers = subs
          .map(sub => mdStore.getLastMarketData(sub))
          .filter(md => (!!md))
          ;
        return tickers;
      } catch (error) {
        logError('getLastMarketDatas(): %o', error);
        throw error;
      }
    };

    const getInstruments = async (symbols) => {
      try {
        const INSTRUMENT = smartwinDB.collection('INSTRUMENT');
        const query = { instrumentid: { $in: symbols } };
        const projection = { _id: 0 };
        const instruments = await INSTRUMENT.find(query, projection).toArray();
        debug('instruments %o', instruments.map(({ instrumentid, volumemultiple }) => ({ instrumentid, volumemultiple })));

        instruments.map((ins) => {
          ins.updatedate = ins.updatedate.toISOString();
          if ('update_date' in ins) delete ins.update_date;
          return ins;
        });
        return instruments;
      } catch (error) {
        logError('getInstruments(): %o', error);
        throw error;
      }
    };

    const marketDataBase = {
      config,
      init,
      dataTypeToDataFeedName,
      subscribeMarketData,
      unsubscribeMarketData,
      getDataFeed,
      getLastMarketDatas,
      getInstruments,
    };
    const marketData = marketDataBase;
    return marketData;
  } catch (error) {
    debug('createMarketData(): %o', error);
    throw error;
  }
}
