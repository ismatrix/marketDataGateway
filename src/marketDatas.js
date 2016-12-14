import createDebug from 'debug';
import createMarketData from './marketData';
import dataFeeds from './dataFeeds';

const logError = createDebug('app:marketDatas:error');
logError.log = console.error.bind(console);

const marketDatasArr = [];

const matchMarketData = newConfig => elem => (
  elem.config.serviceName === newConfig.serviceName
);

async function addMarketData(config) {
  try {
    const existingMarketData = marketDatasArr.find(matchMarketData(config));
    if (existingMarketData !== undefined) return;

    for (const dataFeedConfig of config.dataFeeds) {
      dataFeeds.addDataFeed(dataFeedConfig);
    }

    const newMarketData = createMarketData(config);

    await newMarketData.init();

    marketDatasArr.push(newMarketData);
  } catch (error) {
    logError('addMarketData(): %o', error);
    throw error;
  }
}

function getMarketData(marketDataName) {
  try {
    const config = {
      serviceName: marketDataName,
    };
    const theMarketData = marketDatasArr.find(matchMarketData(config));
    if (theMarketData !== undefined) return theMarketData;

    throw new Error('marketData not found');
  } catch (error) {
    logError('getMarketData(): %o', error);
    throw error;
  }
}

function getMarketDatasConfigs() {
  try {
    const marketDatasConfigs = marketDatasArr.map(elem => ({
      serviceName: elem.config.serviceName,
      dataFeeds: elem.config.dataFeeds.map(df => ({
        name: df.name,
        dataTypes: df.dataTypes,
        server: df.server,
      })),
    })
    );
    return marketDatasConfigs;
  } catch (error) {
    logError('getMarketDatasConfigs(): %o', error);
    throw error;
  }
}

const marketDatas = {
  addMarketData,
  getMarketData,
  getMarketDatasConfigs,
};

export default marketDatas;
