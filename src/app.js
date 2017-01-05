import createDebug from 'debug';
import fs from 'fs';
import path from 'path';
import grpc from 'grpc';
import program from 'commander';
import { upperFirst } from 'lodash';
import mongodb from 'sw-mongodb';
import marketDataGatewayGrpc from './marketDataGateway.grpc';
import config from './config';
import marketDatas from './marketDatas';

program
  .version('1.0.2')
  .option('-c, --credentials-name [value]', 'the name of the server ssl credentials .crt/.key')
  .parse(process.argv);

const grpcUrl = `${config.grpcConfig.ip}:${config.grpcConfig.port}`;
const debug = createDebug(`app:main:${grpcUrl}`);
const logError = createDebug(`app:main:${grpcUrl}:error`);
logError.log = console.error.bind(console);
process
  .on('uncaughtException', error => logError('process.on(uncaughtException): %o', error))
  .on('warning', warning => logError('process.on(warning): %o', warning))
  ;

async function init() {
  try {
    mongodb.setURL(config.mongodbURL);
    const initMarketDatasReport = await Promise.all(config.marketDataConfigs.map(
      conf => marketDatas.addMarketData(conf).catch((error) => {
        logError('init1(): %o', error);
        return `failed adding ${conf.serviceName}`;
      })),
    );
    debug('initMarketDatasReport %o', initMarketDatasReport);
  } catch (error) {
    logError('init(): %o', error);
  }
}

async function main() {
  try {
    debug('app.js main');
    debug('main config %o', config);
    debug('marketDataConfigs %o', config.marketDataConfigs);
    await init();

    const marketDataProto = grpc.load(__dirname.concat('/marketDataGateway.proto'));

    const credentialsName = program.credentialsName || 'localhost';
    const sslServerCrtPath = path.join(__dirname, `../crt/${credentialsName}.crt`);
    const sslServerKeyPath = path.join(__dirname, `../crt/${credentialsName}.key`);
    const sslServerCrt = fs.readFileSync(sslServerCrtPath);
    const sslServerKey = fs.readFileSync(sslServerKeyPath);

    const sslCreds = grpc.ServerCredentials.createSsl(
      null,
      [{ private_key: sslServerKey, cert_chain: sslServerCrt }],
      true
    );

    const server = new grpc.Server();

    // load marketDataGatewayAdmin service
    server.addProtoService(
      marketDataProto.marketDataGatewayAdmin.MarketDataGatewayAdmin.service,
      marketDataGatewayGrpc.marketDataGatewayAdmin,
    );

    // load unique marketData interface service
    for (const mdConfig of config.marketDataConfigs) {
      debug('config %o', config);
      server.addProtoService(
        marketDataProto[mdConfig.serviceName][upperFirst(mdConfig.serviceName)].service,
        marketDataGatewayGrpc[mdConfig.serviceName],
      );
    }

    server.bind(`${config.grpcConfig.ip}:${config.grpcConfig.port}`, sslCreds);
    server.start();
  } catch (error) {
    logError('main(): %o', error);
  }
}
main();
