import createDebug from 'debug';
import dataFeeds from './dataFeeds';
import mongodb from './mongodb';

export default function createMarketData(config) {
  const {
    name,
  } = config;

  const debug = createDebug(`marketData ${name}`);
  const smartwinDB = mongodb.getdb();

  try {
    config.dataFeeds.map(dataFeedConfig => dataFeeds.addDataFeed(dataFeedConfig));

    const init = async () => {
      try {
        const connectPromises = config.dataFeeds
          .map(dataFeedConfig => dataFeeds.getDataFeed(dataFeedConfig.name))
          .map(dataFeed => dataFeed.connect())
          ;
        debug('connectPromises %o', connectPromises);
        await Promise.all(connectPromises);
      } catch (error) {
        debug('Error init(): %o', error);
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
        debug('Error getDataFeed(): %o', error);
      }
    };

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

    const getLastMarketData = (sub) => {
      try {
        debug('getLastMarketData() sub %o', sub);
        const marketDataStore = dataFeeds.getMarketData();
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
      init,
      getDataFeed,
      getLastMarketData,
      getInstruments,
    };
    const marketData = marketDataBase;
    return marketData;
  } catch (error) {
    debug(`createMarketData() ${name}@${config.dataFeed.name} Error: %o`, error);
  }
}
