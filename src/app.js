import logger from 'sw-common';
import fs from 'fs';
import path from 'path';
import grpc from 'grpc';
import program from 'commander';
import { upperFirst } from 'lodash';
import mongodb from 'sw-mongodb';
import crud from 'sw-mongodb-crud';
import can from 'sw-can';
import marketDataGatewayGrpc from './marketDataGateway.grpc';
import config from './config';
import marketDatas from './marketDatas';

program
  .version('1.0.2')
  .option('-c, --credentials-name [value]', 'the name of the server ssl credentials .crt/.key')
  .parse(process.argv);

// const grpcUrl = `${config.grpcConfig.ip}:${config.grpcConfig.port}`;
// const logger.debug = createDebug(`app:main:${grpcUrl}`);
// const logger.error = createDebug(`app:main:${grpcUrl}:error`);
// logger.error.log = console.error.bind(console);
// process
//   .on('uncaughtException', error => logger.error('process.on(uncaughtException): %j', error))
//   .on('warning', warning => logger.error('process.on(warning): %j', warning))
//   ;

async function init() {
  try {
    const initMarketDatasReport = await Promise.all(config.marketDataConfigs.map(
      conf => marketDatas.addMarketData(conf).catch((error) => {
        logger.error('init1(): %j', error);
        return `failed adding ${conf.serviceName}`;
      })),
    );
    logger.debug('initMarketDatasReport %j', initMarketDatasReport);
  } catch (error) {
    logger.error('init(): %j', error);
  }
}

async function main() {
  try {
    logger.debug('app.js main');
    logger.debug('main config %j', config);
    logger.debug('marketDataConfigs %j', config.marketDataConfigs);

    const dbInstance = await mongodb.getDB(config.mongodbURL);
    crud.setDB(dbInstance);

    // init can module with ACL
    const acl = await dbInstance.collection('ACL').find().toArray();
    can.init({ jwtSecret: config.jwtSecret, acl });

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
      true,
    );

    const server = new grpc.Server();

    // load marketDataGatewayAdmin service
    server.addService(
      marketDataProto.marketDataGatewayAdmin.MarketDataGatewayAdmin.service,
      marketDataGatewayGrpc.marketDataGatewayAdmin,
    );

    // load unique marketData interface service
    config.marketDataConfigs.forEach((mdConfig) => {
      logger.debug('config %j', config);
      server.addService(
        marketDataProto[mdConfig.serviceName][upperFirst(mdConfig.serviceName)].service,
        marketDataGatewayGrpc[mdConfig.serviceName],
      );
    });

    server.bind(`${config.grpcConfig.ip}:${config.grpcConfig.port}`, sslCreds);
    server.bind(`${config.grpcConfig.ip}:60052`, grpc.ServerCredentials.createInsecure());
    server.start();
  } catch (error) {
    logger.error('main(): %j', error);
  }
}
main();
