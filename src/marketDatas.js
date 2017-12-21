import logger from 'sw-common';
import createMarketData from './marketData';

// 管理 marketData 在一个队列里面
// const logger.error = createDebug('app:marketDatas:error');
// logger.error.log = console.error.bind(console);

const marketDatasArr = [];

const matchMarketData = newConfig => elem => (
  elem.config.serviceName === newConfig.serviceName
);

async function addMarketData(config) {
  try {
    const existingMarketData = marketDatasArr.find(matchMarketData(config));
    if (existingMarketData !== undefined) return;

    const newMarketData = createMarketData(config);

    await newMarketData.init();

    marketDatasArr.push(newMarketData);
  } catch (error) {
    logger.error('addMarketData(): %j', error);
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
    logger.error('getMarketData(): %j', error);
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
    }),
    );
    return marketDatasConfigs;
  } catch (error) {
    logger.error('getMarketDatasConfigs(): %j', error);
    throw error;
  }
}

const marketDatas = {
  addMarketData,
  getMarketData,
  getMarketDatasConfigs,
};

export default marketDatas;
