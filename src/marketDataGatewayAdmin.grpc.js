import createDebug from 'debug';
import marketDatas from './marketDatas';
import subStores from './subscriptionStores';
import grpcCan from './acl';

const debug = createDebug('app:marketDataGatewayAdmin.grpc');
const logError = createDebug('app:marketDataGatewayAdmin.grpc:error');
logError.log = console.error.bind(console);

async function getMarketDatasConfigs(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');
    debug('getMarketDatasConfigs()');

    const marketDatasConfigs = marketDatas.getMarketDatasConfigs();

    callback(null, { marketDatasConfigs });
  } catch (error) {
    logError('getMarketDatasConfigs %o', error);
    callback(error);
  }
}

async function getSubscriptionsStores(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');
    debug('getSubscriptionsStores()');

    const subscriptionsStores = subStores.getSubscriptionsStores();

    callback(null, { subscriptionsStores });
  } catch (error) {
    logError('getSubscriptionsStores %o', error);
    callback(error);
  }
}

const marketDataGatewayAdmin = {
  getMarketDatasConfigs,
  getSubscriptionsStores,
};

export default marketDataGatewayAdmin;
