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
          ],
          server: {
            ip: '120.76.98.94',
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
          ],
          server: {
            ip: '120.76.98.94',
            port: '10001',
          },
        },
        {
          name: 'mongodb',
          dataTypes: ['dayBar', 'bar'],
          dataDescriptions: [
            { dataType: 'dayBar', resolution: 'snapshot', mode: 'live' },
            { dataType: 'dayBar', resolution: 'snapshot', mode: 'past' },
            { dataType: 'bar', resolution: 'day', mode: 'past' },
          ],
          dbName: 'smartwin',
          collectionName: 'DAYBAR',
          queryInterval: 5000,
          server: {
            ip: '127.0.0.1',
            port: '27017',
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
  }
);

const config = process.env.NODE_ENV === 'development' ? developmentConfig : productionConfig;

export default config;
