import logger from 'sw-common';
import createIceLiveDataFeed from 'sw-datafeed-icelive';
import createIcePastDataFeed from 'sw-datafeed-icepast';
import createMongodbDataFeed from 'sw-datafeed-mongodb';

// 封装链接

export default function createDataFeed(config) {
  try {
    const {
      name,
    } = config;

    let dataFeed;

    switch (name) {
      case 'iceLive':
        dataFeed = createIceLiveDataFeed(config);
        break;
      case 'icePast':
        dataFeed = createIcePastDataFeed(config);
        break;
      case 'mongodb':
        dataFeed = createMongodbDataFeed(config);
        break;
      default:
        throw new Error('Missing dataFeed provider parameter');
    }

    const getLiveDataTypeNames = () => {
      try {
        const liveDataTypeNames = config.dataDescriptions
          .filter(elem => elem.mode === 'live')
          .map(elem => elem.dataType)
          ;

        return liveDataTypeNames;
      } catch (error) {
        logger.error('getLiveDataTypeNames(): %j', error);
        throw error;
      }
    };

    const mdToSubID = (md) => {
      try {
        const subID = `${name}:${md.dataType}:${md.resolution}:${md.symbol}`;
        return subID;
      } catch (error) {
        logger.error('mdToSubID(): %j', error);
        throw error;
      }
    };

    const dataFeedBase = {
      config,
      getLiveDataTypeNames,
      mdToSubID,
    };


    return Object.assign(dataFeed, dataFeedBase);
  } catch (error) {
    logger.error('createDataFeed(): %j', error);
    throw error;
  }
}
