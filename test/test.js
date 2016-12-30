// import createDebug from 'debug';
import fs from 'fs';
import path from 'path';
import createGrpcClient from 'sw-grpc-client';

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

const symbols = ['ag1712'];

const marketDepthSubscriptions = [
    { symbol: 'ag1712', resolution: 'snapshot', dataType: 'marketDepth' },
];
const barSubscriptions = [
    { symbol: 'ag1712', resolution: 'minute', dataType: 'bar' },
];
const tickerSubscriptions = [
    { symbol: 'ag1712', resolution: 'snapshot', dataType: 'ticker' },
];
const dayBarSubscriptions = [
    { symbol: 'ag1712', resolution: 'snapshot', dataType: 'dayBar' },
];

describe('#getMarketDepthStream()', () => {
  it('success', () => md.getMarketDepthStream({}).on('data', () => {}));
});

describe('#getBarStream()', () => {
  it('success', () => md.getBarStream({}).on('data', () => {}));
});

describe('#getTickerStream()', () => {
  it('success', () => md.getTickerStream({}).on('data', () => {}));
});

describe('#getDayBarStream()', () => {
  it('success', () => md.getDayBarStream({}).on('data', () => {}));
});

describe('#getPastBarStream()', () => {
  it('success', () => new Promise((resolve, reject) => {
    md.getPastBarStream({
      symbol: 'IF1608',
      dataType: 'bar',
      resolution: 'minute',
      startDate: '2016-08-01',
      endDate: '2016-08-01',
    })
    .on('data', () => {})
    .on('end', () => resolve())
    .on('error', error => reject(error))
    ;
  }));
});

describe('#getPastTickerStream()', () => {
  it('success', () => new Promise((resolve, reject) => {
    md.getPastTickerStream({
      symbol: 'IF1608',
      dataType: 'ticker',
      resolution: 'snapshot',
      startDate: '2016-08-01',
      endDate: '2016-08-01',
    })
    .on('data', () => resolve())
    .on('end', () => resolve())
    .on('error', error => reject(error))
    ;
  }));
});

describe('#getPastDayBarStream()', () => {
  it('success', () => new Promise((resolve, reject) => {
    md.getPastDayBarStream({
      symbol: 'IF1608',
      dataType: 'dayBar',
      resolution: 'snapshot',
      startDate: '2016-08-01',
      endDate: '2016-08-01',
    })
    .on('data', () => {})
    .on('end', () => resolve())
    .on('error', error => reject(error))
    ;
  }));
});

describe('#subscribeMarketData()', () => {
  it('success', () => md.subscribeMarketData({ symbol: 'IF1701', resolution: 'minute', dataType: 'bar' }));
});

describe('#unsubscribeMarketData()', () => {
  it('success', () => md.unsubscribeMarketData({ symbol: 'IF1701', resolution: 'minute', dataType: 'bar' }));
});

describe('#getLastMarketDepths()', () => {
  it('success', () => md.getLastMarketDepths({ subscriptions: marketDepthSubscriptions }));
});

describe('#getLastBars()', () => {
  it('success', () => md.getLastBars({ subscriptions: barSubscriptions }));
});

describe('#getLastTickers()', () => {
  it('success', () => md.getLastTickers({ subscriptions: tickerSubscriptions }));
});

describe('#getLastDayBars()', () => {
  it('success', () => md.getLastDayBars({ subscriptions: dayBarSubscriptions }));
});

describe('#getInstruments()', () => {
  it('success', () => md.getInstruments({ symbols }));
});

describe('#getSubscribableDataDescriptions()', () => {
  it('success', () => md.getSubscribableDataDescriptions());
});

describe('#getMySubscriptions()', () => {
  it('success', () => md.getMySubscriptions());
});
