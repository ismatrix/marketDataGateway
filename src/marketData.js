import createDebug from 'debug';
import { reduce } from 'lodash';
import mongodb from 'sw-mongodb';
import dataFeeds from './dataFeeds';

const debug = createDebug('app:marketData');
const logError = createDebug('app:marketData:error');
logError.log = console.error.bind(console);

let smartwinDB;

const matchDataDescription = newDesc => desc => (
  desc.mode === newDesc.mode &&
  desc.resolution === newDesc.resolution &&
  desc.dataType === newDesc.dataType);


export default function createMarketData(config) {
  try {
    const init = async () => {
      try {
        smartwinDB = await mongodb.getDB();
        const addDataFeedPromises = config.dataFeeds
          .map(dataFeedConfig => dataFeeds.addDataFeed(dataFeedConfig).catch(error => logError('failed adding dataFeed %o with error: %o', dataFeedConfig.name, error)))
          ;

        debug('connectPromises %o', addDataFeedPromises);
        const initReport = await Promise.all(addDataFeedPromises);
        return initReport;
      } catch (error) {
        logError('init(): %o', error);
        throw error;
      }
    };

    const getOwnDataFeeds = () => {
      try {
        const dataFeedNames = config.dataFeeds.map(conf => conf.name);
        const ownDataFeeds = dataFeeds.getDataFeedsByNames(dataFeedNames);
        return ownDataFeeds;
      } catch (error) {
        logError('getOwnDataFeeds(): %o', error);
        throw error;
      }
    };

    const getOwnDataFeedConfigs = () => {
      try {
        const ownDataFeedConfigs = getOwnDataFeeds().map(conf => conf.config);
        return ownDataFeedConfigs;
      } catch (error) {
        logError('getOwnDataFeedConfigs(): %o', error);
        throw error;
      }
    };

    const dataTypeToDataFeedName = (dataType) => {
      try {
        const dataFeedConfigs = getOwnDataFeeds().map(conf => conf.config);
        for (const dataFeedConfig of dataFeedConfigs) {
          if (dataFeedConfig.dataTypes.includes(dataType)) return dataFeedConfig.name;
        }
        throw new Error('dataType not found in own dataFeed configs');
      } catch (error) {
        logError('dataTypeToDataFeedName(): %o', error);
        throw error;
      }
    };

    const isExistingDataDescription = (dataDescription) => {
      try {
        const dataFeedConfig = getOwnDataFeedConfigs().find(
          conf => conf.dataDescriptions.find(matchDataDescription(dataDescription)));
        if (dataFeedConfig) return true;
        return false;
      } catch (error) {
        logError('isExistingDataDescription(): %o', error);
        throw error;
      }
    };

    const subscriptionToDataDescription = (sub) => {
      try {
        const dataDescription = {};
        dataDescription.dataType = sub.dataType;
        dataDescription.resolution = sub.resolution;
        dataDescription.mode = (sub.startDate || sub.endDate) ? 'past' : 'live';
        debug('dataDescription: %o', dataDescription);

        return dataDescription;
      } catch (error) {
        logError('subscriptionToDataDescription(): %o', error);
        throw error;
      }
    };

    const isValidSubscription = (sub) => {
      try {
        const dataDescription = subscriptionToDataDescription(sub);
        if (!isExistingDataDescription(dataDescription)) return false;
        return true;
      } catch (error) {
        logError('isValidSubscription(): %o', error);
        throw error;
      }
    };

    const getDataFeedConfigByDataDescription = (dataDescription) => {
      try {
        const dataFeedConfig = getOwnDataFeedConfigs().find(
          conf => conf.dataDescriptions.find(matchDataDescription(dataDescription)));

        if (dataFeedConfig) return dataFeedConfig;

        const flatDataDescription = reduce(dataDescription, (acc, cur) => acc.concat(cur, ':'), '');
        throw new Error(`dataDescription ${flatDataDescription} not found in dataFeeds config`);
      } catch (error) {
        logError('getDataFeedConfigByDataDescription(): %o', error);
        throw error;
      }
    };

    const getDataFeedByDataDescription = (dataDescription) => {
      try {
        const dataFeedConfig = getDataFeedConfigByDataDescription(dataDescription);
        const dataFeed = dataFeeds.getDataFeed(dataFeedConfig.name);
        return dataFeed;
      } catch (error) {
        logError('getDataFeedByDataDescription(): %o', error);
        throw error;
      }
    };

    const getDataFeedBySubscription = (sub) => {
      try {
        if (!isValidSubscription(sub)) {
          const flatSubscritpion = reduce(sub, (acc, cur) => acc.concat(cur, ':'), '');
          throw new Error(`subscription is not valid: ${flatSubscritpion}`);
        }
        const dataDescription = subscriptionToDataDescription(sub);
        const dataFeed = getDataFeedByDataDescription(dataDescription);
        return dataFeed;
      } catch (error) {
        logError('getDataFeedBySubscription(): %o', error);
        throw error;
      }
    };

    const getDataFeedByDataType = (dataType) => {
      try {
        const dataFeedConfig = getOwnDataFeedConfigs().find(
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

    const getInstruments = async (symbols) => {
      try {
        const INSTRUMENT = smartwinDB.collection('INSTRUMENT');
        const query = { instrumentid: { $in: symbols } };
        const projection = { _id: 0 };
        const instruments = await INSTRUMENT.find(query, projection).toArray();

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

    const getSubscribableDataDescriptions = async () => {
      try {
        const dataDescriptions = getOwnDataFeedConfigs()
          .filter(dataFeedConf => !!dataFeedConf.dataDescriptions)
          .reduce((acc, cur) => acc.concat(cur.dataDescriptions), [])
          ;
        return dataDescriptions;
      } catch (error) {
        logError('getSubscribableDataDescriptions(): %o', error);
        throw error;
      }
    };

    const marketDataBase = {
      config,
      init,
      dataTypeToDataFeedName,
      getDataFeedByDataType,
      getDataFeedByDataDescription,
      getDataFeedBySubscription,
      getInstruments,
      getSubscribableDataDescriptions,
    };
    const marketData = marketDataBase;
    return marketData;
  } catch (error) {
    debug('createMarketData(): %o', error);
    throw error;
  }
}
