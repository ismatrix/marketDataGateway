import createDebug from 'debug';
import createIceLiveDataFeed from 'sw-datafeed-icelive';

export default function createDataFeed(config) {
  const {
    name,
    server,
  } = config.dataFeed;

  const debug = createDebug(`dataFeed ${name}@${server.ip}:${server.port}`);
  try {
    let dataFeed;

    switch (name) {
      case 'iceLive':
        dataFeed = createIceLiveDataFeed(config);
        break;
      default:
        throw new Error('Missing dataFeed provider parameter');
    }

    return dataFeed;
  } catch (error) {
    debug('createDataFeed() Error: %o', error);
  }
}
