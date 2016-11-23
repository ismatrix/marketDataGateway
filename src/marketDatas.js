import createDebug from 'debug';
import createMarketData from './marketData';

const debug = createDebug('marketDatas');

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
    debug('Error addMarketData(): %o', error);
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
    debug('Error getMarketData(): %o', error);
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
    debug('Error getMarketDatasConfigs(): %o', error);
  }
}

const marketDatas = {
  addMarketData,
  getMarketData,
  getMarketDatasConfigs,
};

export default marketDatas;
