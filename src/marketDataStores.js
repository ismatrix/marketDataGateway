import createDebug from 'debug';
import createMarketDataStore from './marketDataStore';

const debug = createDebug('marketDataStores');

const mdStores = [];

const matchMdStore = newConfig => elem => (
  elem.config.dataFeed.config.name === newConfig.dataFeed.config.name
);

function addAndGetMdStore(config) {
  try {
    const existingMdStore = mdStores.find(matchMdStore(config));
    debug('existingMdStore %o', existingMdStore);
    if (existingMdStore !== undefined) return existingMdStore;

    const newMdStore = createMarketDataStore(config);
    mdStores.push(newMdStore);
    return newMdStore;
  } catch (error) {
    debug('Error addAndGetMdStore(): %o', error);
  }
}

function getMdStoreByName(mdStoreName) {
  try {
    const config = {
      dataFeed: {
        config: {
          name: mdStoreName,
        },
      },
    };

    const existingMdStore = mdStores.find(matchMdStore(config));
    if (existingMdStore !== undefined) return existingMdStore;

    throw new Error('marketDataStore not found');
  } catch (error) {
    debug('Error getMdStoreByName(): %o', error);
  }
}

const marketDataStoresBase = {
  addAndGetMdStore,
  getMdStoreByName,
};

export default marketDataStoresBase;
