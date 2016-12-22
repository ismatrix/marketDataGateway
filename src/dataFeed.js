import createDebug from 'debug';
import createIceLiveDataFeed from 'sw-datafeed-icelive';
import createIcePastDataFeed from 'sw-datafeed-icepast';
import createMongodbDataFeed from 'sw-datafeed-mongodb';

const logError = createDebug('app:dataFeed:error');
logError.log = console.error.bind(console);

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
        logError('getLiveDataTypeNames(): %o', error);
        throw error;
      }
    };

    const mdToSubID = (md) => {
      try {
        const subID = `${name}:${md.dataType}:${md.resolution}:${md.symbol}`;
        return subID;
      } catch (error) {
        logError('mdToSubID(): %o', error);
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
    logError('createDataFeed(): %o', error);
    throw error;
  }
}
