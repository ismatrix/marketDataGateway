const productionConfig = {
  jwtSecret: 'Ci23fWtahDYE3dfirAHrJhzrUEoslIxqwcDN9VNhRJCWf8Tyc1F1mqYrjGYF',
  mongodbURL: 'mongodb://127.0.0.1:27017/smartwin',
  grpcConfig: {
    ip: '0.0.0.0',
    port: '50052',
  },
  redisConfig: {
    port: 6379,
    constants: ['globallyUnused', 'globallySubscribed'],
    keys: {
      subID: {
        subKeyDefs: ['dataFeedName', 'dataType', 'resolution', 'symbol'],
        valueDefs: ['sessionIDs', 'md', 'lastMd'],
      },
      subsInfo: {
        subKeyDefs: ['infoName'],
        valueDefs: ['subIDs'],
      },
      dataType: {
        subKeyDefs: ['dataType'],
        valueDefs: ['sessionIDs'],
      },
    },
  },
  marketDataConfigs: [
    {
      serviceName: 'smartwinFuturesMd',
      dataFeeds: [
        {
          name: 'icePast',
          dataTypes: ['ticker', 'bar'],
          dataDescriptions: [
            { dataType: 'ticker', resolution: 'snapshot', mode: 'past' },
            { dataType: 'bar', resolution: 'minute', mode: 'past' },
            { dataType: 'dayBar', resolution: 'day', mode: 'past' },
          ],
          server: {
            ip: 'quantowin.com',
            port: '10101',
          },
        },
        {
          name: 'iceLive',
          dataTypes: ['ticker', 'bar', 'marketDepth'],
          dataDescriptions: [
            { dataType: 'marketDepth', resolution: 'snapshot', mode: 'live' },
            { dataType: 'ticker', resolution: 'snapshot', mode: 'live' },
            { dataType: 'bar', resolution: 'minute', mode: 'live' },
            { dataType: 'dayBar', resolution: 'day', mode: 'live' },
          ],
          server: {
            ip: 'quantowin.com',
            port: '10001',
          },
        },
      ],
    },
  ],
};

const developmentConfig = Object.assign({},
  productionConfig,
  {
    mongodbURL: 'mongodb://127.0.0.1:27018/smartwin',
  },
);
developmentConfig.marketDataConfigs
  .filter(element => element.serviceName === 'smartwinFuturesMd')
  .forEach(elem =>
    elem.dataFeeds.filter(el => el.name === 'mongodb').forEach((e) => { e.server.port = '27018'; }),
  )
  ;

const config = process.env.NODE_ENV === 'development' ? developmentConfig : productionConfig;

export default config;
