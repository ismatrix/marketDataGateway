import logger from 'sw-common';
import can from 'sw-can';
import marketDatas from './marketDatas';

// const logger.debug = createDebug('app:marketDataGatewayAdmin.grpc');
// const logger.error = createDebug('app:marketDataGatewayAdmin.grpc:error');
// logger.error.log = console.error.bind(console);

async function getMarketDatasConfigs(call, callback) {
  try {
    await can.grpc(call, 'get', 'marketDataGateway/configs');
    logger.debug('getMarketDatasConfigs()');

    const marketDatasConfigs = marketDatas.getMarketDatasConfigs();

    callback(null, { marketDatasConfigs });
  } catch (error) {
    logger.error('getMarketDatasConfigs %j', error);
    callback(error);
  }
}

const marketDataGatewayAdmin = {
  getMarketDatasConfigs,
};

export default marketDataGatewayAdmin;
