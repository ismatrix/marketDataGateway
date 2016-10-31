export const jwtSecret = 'Ci23fWtahDYE3dfirAHrJhzrUEoslIxqwcDN9VNhRJCWf8Tyc1F1mqYrjGYF';

export const mongodbUrl = 'mongodb://127.0.0.1:27017/smartwin';

export const grpcConfig = {
  ip: '0.0.0.0',
  port: '50052',
};

export const marketDataConfigs = [
  {
    name: 'smartwinFutures',
    dataFeed: {
      name: 'iceLive',
      server: {
        ip: '120.76.98.94',
        port: '4502',
      },
    },
  },
];
