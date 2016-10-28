import createDebug from 'debug';
import createMarketData from './marketData';

const debug = createDebug('marketDatas');

const marketDatasArr = [];

async function addMarketData(config) {
  try {
    if (marketDatasArr.map(elem => elem.config.name).includes(config.name)) return;

    const newMarketData = createMarketData(config);
    newMarketData.config = config;

    await newMarketData.connect();

    marketDatasArr.push(newMarketData);
  } catch (error) {
    debug('Error addMarketData(): %o', error);
  }
}

function getMarketData(marketDataName) {
  try {
    const theMarketData = marketDatasArr.find(elem => elem.config.name === marketDataName);
    if (theMarketData !== undefined) return theMarketData;

    throw new Error('marketData not found');
  } catch (error) {
    debug('Error getMarketData(): %o', error);
  }
}

const marketDatas = {
  addMarketData,
  getMarketData,
};

export default marketDatas;
