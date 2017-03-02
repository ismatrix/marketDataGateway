import createDebug from 'debug';
import fs from 'fs';
import path from 'path';
import createGrpcClient from 'sw-grpc-client';

const debug = createDebug('marketDataGatewat-test');
const jwtoken = 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJfaWQiOiI1NzZhNDNjNjUyNmRjZWRjMDcwMjg4YjMiLCJ1c2VyaWQiOiJ2aWN0b3IiLCJkcHQiOlsi57O757uf6YOoIl0sImlhdCI6MTQ2NzE2NDg5Mn0.-ousXclNcnTbIDTJPJWnAkVVPErPw418TMKDqpWlZO0';

const sslCaCrtPath = path.join(__dirname, '../crt/rootCA.pem');
const sslCaCrt = fs.readFileSync(sslCaCrtPath);

const md = createGrpcClient({
  serviceName: 'smartwinFuturesMd',
  server: {
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
// .on('data', data => debug('data %o', data))
// .on('end', () => debug('end'))
// .on('error', error => debug('error %o', error))
// ;

md.getPastBarStream({
  symbol: 'ag1705',
  dataType: 'bar',
  resolution: 'day',
  startDate: '2017-01-16',
  endDate: '2017-01-26',
})
.on('data', data => debug('data %o', data))
.on('end', () => debug('end'))
.on('error', error => debug('error %o', error))
;
