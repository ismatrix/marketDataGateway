import createDebug from 'debug';
import marketDatas from './marketDatas';
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

const marketDataGatewayAdmin = {
  getMarketDatasConfigs,
};

export default marketDataGatewayAdmin;
