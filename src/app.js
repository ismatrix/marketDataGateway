import createDebug from 'debug';
import fs from 'fs';
import path from 'path';
import grpc from 'grpc';
import liveMarketData from './liveMarketData.grpc';
import mongodb from './mongodb';
import {
  mongodbUrl,
  marketDataConfigs,
  grpcConfig,
} from './config';
import marketDatas from './marketDatas';

const debug = createDebug('app');

async function init() {
  try {
    await Promise.all([
      mongodb.connect(mongodbUrl),
      marketDatas.addMarketData(marketDataConfigs[0]),
    ]);
  } catch (error) {
    debug('Error init(): %o', error);
  }
}

async function main() {
  try {
    debug('app.js main');
    debug('marketDataConfigs[0] %o', marketDataConfigs[0]);
    await init();

    const marketDataProto = grpc.load(__dirname.concat('/liveMarketData.proto'));

    const sslServerCrtPath = path.join(__dirname, '../crt/server.crt');
    const sslServerKeyPath = path.join(__dirname, '../crt/server.key');
    const sslServerCrt = fs.readFileSync(sslServerCrtPath);
    const sslServerKey = fs.readFileSync(sslServerKeyPath);

    const sslCreds = grpc.ServerCredentials.createSsl(
      null,
      [{ private_key: sslServerKey, cert_chain: sslServerCrt }],
      true
    );

    const server = new grpc.Server();
    server.addProtoService(
      marketDataProto.liveMarketDataPackage.SmartwinFuturesService.service,
      liveMarketData.smartwinFutures
    );

    server.bind(`${grpcConfig.ip}:${grpcConfig.port}`, sslCreds);
    server.start();
  } catch (error) {
    debug('Error main(): %o', error);
  }
}
main();
