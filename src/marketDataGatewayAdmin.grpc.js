import createDebug from 'debug';
import can from 'sw-can';
import marketDatas from './marketDatas';

const debug = createDebug('app:marketDataGatewayAdmin.grpc');
const logError = createDebug('app:marketDataGatewayAdmin.grpc:error');
logError.log = console.error.bind(console);

async function getMarketDatasConfigs(call, callback) {
  try {
    await can.grpc(call, 'get', 'marketDataGateway/configs');
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
