import can from 'sw-can';
import marketDatas from './marketDatas';
import logger from 'sw-common'


async function getMarketDatasConfigs(call, callback) {
  try {
    await can.grpc(call, 'get', 'marketDataGateway/configs');
    logger.info('getMarketDatasConfigs()');

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
