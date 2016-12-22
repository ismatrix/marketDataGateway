export const jwtSecret = 'Ci23fWtahDYE3dfirAHrJhzrUEoslIxqwcDN9VNhRJCWf8Tyc1F1mqYrjGYF';

export const mongodbUrl = 'mongodb://127.0.0.1:27017/smartwin';

export const grpcConfig = {
  ip: '0.0.0.0',
  port: '50052',
};

export const marketDataConfigs = [
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
        dataTypes: ['dayBar'],
        dataDescriptions: [
          { dataType: 'dayBar', resolution: 'snapshot', mode: 'live' },
          { dataType: 'dayBar', resolution: 'snapshot', mode: 'past' },
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
];
