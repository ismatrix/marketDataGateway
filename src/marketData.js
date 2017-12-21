import logger from 'sw-common';
import { reduce } from 'lodash';
import schedule from 'node-schedule';
import dataFeeds from './dataFeeds';

// 封装ｄａｔａｆｅｅｄｓ

const matchDataDescription = newDesc => desc => (
  desc.mode === newDesc.mode
  && newDesc.resolution.includes(desc.resolution)
  && desc.dataType === newDesc.dataType);


export default function createMarketData(config) {
  try {
    let memoryInstruments = [];

    const updateMemoryInstruments = async () => {
      try {
        const icePast = dataFeeds.getDataFeed('icePast');

        const dbInstruments = await icePast.getInstruments({
          isTrading: [1],
          productClasses: ['1'],
        });
        logger.debug('dbInstruments.length: %j', dbInstruments.length);
        // db instrumentname === null not compatible with proto3
        memoryInstruments = dbInstruments.map((ins) => {
          if (ins.instrumentname === null) delete ins.instrumentname;
          delete ins._id;
          delete ins.updatedate;
          return ins;
        });
        logger.debug('memoryInstruments.length: %j', memoryInstruments.length);
      } catch (error) {
        logger.error('updateMemoryInstruments(): %j', error);
        throw error;
      }
    };
    schedule.scheduleJob('01 21 * * 1-5', updateMemoryInstruments);

    const init = async () => {
      try {
        const addDataFeedPromises = config.dataFeeds
          .map(dataFeedConfig => dataFeeds.addDataFeed(dataFeedConfig).catch(error => logger.error('failed adding dataFeed %j with error: %j', dataFeedConfig.name, error)))
          ;

        logger.debug('connectPromises %j', addDataFeedPromises);
        const initReport = await Promise.all(addDataFeedPromises);
        await updateMemoryInstruments();
        return initReport;
      } catch (error) {
        logger.error('init(): %j', error);
        throw error;
      }
    };

    const getOwnDataFeeds = () => {
      try {
        const dataFeedNames = config.dataFeeds.map(conf => conf.name);
        const ownDataFeeds = dataFeeds.getDataFeedsByNames(dataFeedNames);
        return ownDataFeeds;
      } catch (error) {
        logger.error('getOwnDataFeeds(): %j', error);
        throw error;
      }
    };

    const getOwnDataFeedConfigs = () => {
      try {
        const ownDataFeedConfigs = getOwnDataFeeds().map(conf => conf.config);
        return ownDataFeedConfigs;
      } catch (error) {
        logger.error('getOwnDataFeedConfigs(): %j', error);
        throw error;
      }
    };

    const dataTypeToDataFeedName = (dataType) => {
      try {
        const dataFeedConfigs = getOwnDataFeeds().map(conf => conf.config);
        dataFeedConfigs.forEach((dataFeedConfig) => {
          if (dataFeedConfig.dataTypes.includes(dataType)) return dataFeedConfig.name;
        });
        throw new Error('dataType not found in own dataFeed configs');
      } catch (error) {
        logger.error('dataTypeToDataFeedName(): %j', error);
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
        logger.error('isExistingDataDescription(): %j', error);
        throw error;
      }
    };

    const subscriptionToDataDescription = (sub) => {
      try {
        const dataDescription = {};
        dataDescription.dataType = sub.dataType;
        dataDescription.resolution = sub.resolution;
        dataDescription.mode = (sub.startDate || sub.endDate) ? 'past' : 'live';
        // logger.debug('dataDescription: %j', dataDescription);

        return dataDescription;
      } catch (error) {
        logger.error('subscriptionToDataDescription(): %j', error);
        throw error;
      }
    };

    const isValidSubscription = (sub) => {
      try {
        const dataDescription = subscriptionToDataDescription(sub);
        if (!isExistingDataDescription(dataDescription)) return false;
        return true;
      } catch (error) {
        logger.error('isValidSubscription(): %j', error);
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
        logger.error('getDataFeedConfigByDataDescription(): %j', error);
        throw error;
      }
    };

    const getDataFeedByDataDescription = (dataDescription) => {
      try {
        const dataFeedConfig = getDataFeedConfigByDataDescription(dataDescription);
        const dataFeed = dataFeeds.getDataFeed(dataFeedConfig.name);
        return dataFeed;
      } catch (error) {
        logger.error('getDataFeedByDataDescription(): %j', error);
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
        logger.error('getDataFeedBySubscription(): %j', error);
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
        logger.error('getDataFeedByDataType(): %j', error);
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
        logger.error('getSubscribableDataDescriptions(): %j', error);
        throw error;
      }
    };

    const getMemoryInstruments = (filter) => {
      try {
        const filteredMemoryInstruments = memoryInstruments.filter(ins => (
          (filter.symbols.length === 0 || filter.symbols.includes(ins.instrumentid))
          && (filter.products.length === 0 || filter.products.includes(ins.productid))
          && (filter.exchanges.length === 0 || filter.exchanges.includes(ins.exchangeid))
          && (filter.ranks.length === 0 || filter.ranks.includes(ins.rank))
          &&
          (filter.productClasses.length === 0 || filter.productClasses.includes(ins.productclass))
          && (filter.isTrading.length === 0 || filter.isTrading.includes(ins.istrading))
        ));
        return filteredMemoryInstruments;
      } catch (error) {
        logger.error('getMemoryInstruments(): %j', error);
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
      getSubscribableDataDescriptions,
      getMemoryInstruments,
    };
    const marketData = marketDataBase;
    return marketData;
  } catch (error) {
    logger.debug('createMarketData(): %j', error);
    throw error;
  }
}
