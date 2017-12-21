import logger from 'sw-common';
import fs from 'fs';
import path from 'path';
import createGrpcClient from 'sw-grpc-client';

// const logger.debug = createDebug('marketDataGatewat-test');
const jwtoken = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJfaWQiOiI1NzZhNDNjNjUyNmRjZWRjMDcwMjg4YjMiLCJ1c2VyaWQiOiJ2aWN0b3IiLCJkcHQiOlsi57O757uf6YOoIl0sImlhdCI6MTQ2NzE2NDg5Mn0.-ousXclNcnTbIDTJPJWnAkVVPErPw418TMKDqpWlZO0';

const sslCaCrtPath = path.join(__dirname, '../crt/rootCA.pem');
const sslCaCrt = fs.readFileSync(sslCaCrtPath);

const md = createGrpcClient({
  serviceName: 'smartwinFuturesMd',
  server: {
    // ip: 'markets.quantowin.com',
    ip: 'localhost',
    port: '50052',
  },
  jwtoken,
  sslCaCrt,
});

// md.getPastBarStream({
//   symbol: 'IF1608',
//   dataType: 'bar',
//   resolution: 'minute',
//   startDate: '2016-08-01',
//   endDate: '2016-08-01',
// })
// .on('data', data => logger.debug('data %j', data))
// .on('end', () => logger.debug('end'))
// .on('error', error => logger.debug('error %j', error))
// ;

// md.getPastDayBarStream({
//   symbol: 'IF1608',
//   dataType: 'dayBar',
//   resolution: 'day',
//   startDate: '2016-06-27',
//   endDate: '2016-08-19',
// })
// .on('data', data => logger.debug('data %j', data))
// .on('end', () => logger.debug('end'))
// .on('error', error => logger.debug('error %j', error))
// ;

md.getDayBarStream({})
.on('data', data => logger.debug('dayBar %j', data))
.on('end', () => logger.debug('end'))
.on('error', error => logger.debug('error %j', error))
;

md.subscribeMarketData({ symbol: 'IF1608', resolution: 'snapshot', dataType: 'dayBar' });
md.subscribeMarketData({ symbol: 'IF1708', resolution: 'day', dataType: 'dayBar' });
