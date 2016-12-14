import createDebug from 'debug';
import { reduce } from 'lodash';
import mongodb from './mongodb';
import dataFeeds from './dataFeeds';
import subStores from './subscriptionStores';
import mdStores from './marketDataStores';

const debug = createDebug('app:marketData');
const logError = createDebug('app:marketData:error');
logError.log = console.error.bind(console);

const matchDataDescription = newDesc => desc => (
  desc.mode === newDesc.mode &&
  desc.resolution === newDesc.resolution &&
  desc.dataType === newDesc.dataType);


export default function createMarketData(config) {
  try {
    const smartwinDB = mongodb.getdb();

    const init = async () => {
      try {
        const connectPromises = config.dataFeeds
          .map(dataFeedConfig => dataFeeds.getDataFeed(dataFeedConfig.name))
          .map((dataFeed) => {
            if ('connect' in dataFeed) return dataFeed.connect();
            return 'no connect function';
          })
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

    const subscriptionToDataDescription = (sub) => {
      try {
        const dataDescription = {};
        dataDescription.dataType = sub.dataType;
        dataDescription.resolution = sub.resolution;
        dataDescription.mode = (('startDate' in sub) || ('endDate' in sub)) ? 'past' : 'live';
        debug('dataDescription: %o', dataDescription);
        return dataDescription;
      } catch (error) {
        logError('subscriptionToDataDescription(): %o', error);
        throw error;
      }
    };

    const getDataFeedByDataDescription = (dataDescription) => {
      try {
        const dataFeedConfig = config.dataFeeds.find(
          conf => conf.dataDescriptions.find(matchDataDescription(dataDescription)));

        if ('name' in dataFeedConfig) {
          const theDataFeed = dataFeeds.getDataFeed(dataFeedConfig.name);
          return theDataFeed;
        }

        const flatDataDescription = reduce(dataDescription, (acc, cur) => acc.concat(cur, ':'), '');
        throw new Error(`dataDescription ${flatDataDescription} not found in dataFeeds config`);
      } catch (error) {
        logError('getDataFeedByDataDescription(): %o', error);
        throw error;
      }
    };

    const getDataFeedBySubscription = (sub) => {
      try {
        const dataDescription = subscriptionToDataDescription(sub);
        const dataFeed = getDataFeedByDataDescription(dataDescription);
        return dataFeed;
      } catch (error) {
        logError('getDataFeedBySubscription(): %o', error);
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

    const getDataFeedByDataType = (dataType) => {
      try {
        const dataFeedConfig = config.dataFeeds.find(
          dfConfig => dfConfig.dataTypes.includes(dataType));

        if ('name' in dataFeedConfig) {
          const theDataFeed = dataFeeds.getDataFeed(dataFeedConfig.name);
          return theDataFeed;
        }

        throw new Error('No dataFeed for this dataType');
      } catch (error) {
        logError('getDataFeedByDataType(): %o', error);
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
      getDataFeedByDataType,
      getDataFeedByDataDescription,
      getDataFeedBySubscription,
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
