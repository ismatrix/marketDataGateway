import createDebug from 'debug';
import marketDatas from './marketDatas';
import subStores from './subscriptionStores';
import grpcCan from './acl';

const debug = createDebug('marketDataGatewayAdmin.grpc');

async function getMarketDatasConfigs(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    const marketDatasConfigs = marketDatas.getMarketDatasConfigs();

    callback(null, { marketDatasConfigs });
  } catch (error) {
    debug('Error getMarketDatasConfigs %o', error);
    callback(error);
  }
}

async function getSubscriptionsStores(call, callback) {
  try {
    await grpcCan(call, 'read', 'getOrders');

    const subscriptionsStores = subStores.getSubscriptionsStores();

    callback(null, { subscriptionsStores });
  } catch (error) {
    debug('Error getSubscriptionsStores %o', error);
    callback(error);
  }
}

const marketDataGatewayAdmin = {
  getMarketDatasConfigs,
  getSubscriptionsStores,
};

export default marketDataGatewayAdmin;
