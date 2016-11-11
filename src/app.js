import createDebug from 'debug';
import fs from 'fs';
import path from 'path';
import grpc from 'grpc';
import { upperFirst } from 'lodash';
import marketDataGatewayGrpc from './marketDataGateway.grpc';
import mongodb from './mongodb';
import {
  mongodbUrl,
  marketDataConfigs,
  grpcConfig,
} from './config';
import marketDatas from './marketDatas';

const grpcUrl = `${grpcConfig.ip}:${grpcConfig.port}`;
const debug = createDebug(`app ${grpcUrl}`);

async function init() {
  try {
    await mongodb.connect(mongodbUrl);
    await Promise.all([].concat(
      marketDataConfigs.map(conf => marketDatas.addMarketData(conf)),
    ));
  } catch (error) {
    debug('Error init(): %o', error);
  }
}

async function main() {
  try {
    debug('app.js main');
    debug('marketDataConfigs %o', marketDataConfigs);
    await init();

    const marketDataProto = grpc.load(__dirname.concat('/marketDataGateway.proto'));

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
    for (const config of marketDataConfigs) {
      debug('config %o', config);
      server.addProtoService(
        marketDataProto[config.serviceName][upperFirst(config.serviceName)].service,
        marketDataGatewayGrpc[config.serviceName],
      );
    }

    server.bind(`${grpcConfig.ip}:${grpcConfig.port}`, sslCreds);
    server.start();
  } catch (error) {
    debug('Error main(): %o', error);
  }
}
main();
