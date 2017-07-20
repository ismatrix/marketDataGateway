import createDebug from 'debug';
import createRedis from 'redis';
import bluebird from 'bluebird';
import config from './config';

// redis的方法
const debug = createDebug('app:redis');
const logError = createDebug('app:redis:error');
logError.log = console.error.bind(console);

bluebird.promisifyAll(createRedis.RedisClient.prototype);
bluebird.promisifyAll(createRedis.Multi.prototype);

function encode(str) {
  return str.replace(
    /[-:|_]/g,
    c => '%'.concat(c.charCodeAt(0).toString(16)),
  );
}

function decode(str) {
  return decodeURI(str);
}

// Redis separators definitions
const ns = {
  KEYDEFANDVALUEDEFSEP: '|',
  NSANDKEYSEP: '-',
  SUBKEYSSEP: ':',
  NSVARSEP: '_',
  NAMESPACE: 'namespace',
  KEYDEF: 'keyDef',
  VALUEDEF: 'valueDef',
  KEY: 'key',
};

function joinNamespace(keyDefinition, valueDefinition) {
  return [keyDefinition, valueDefinition].join(ns.KEYDEFANDVALUEDEFSEP);
}

function joinFullKey(namespace, key) {
  return [namespace, key].join(ns.NSANDKEYSEP);
}
const join = joinFullKey;

function joinSubKeys(...subKeys) {
  return [...subKeys]
    .map(elem => encode(elem))
    .join(ns.SUBKEYSSEP);
}

function getNamespace(fullKey) {
  return fullKey.split(ns.NSANDKEYSEP)[0];
}

function getKey(fullKey) {
  return fullKey.split(ns.NSANDKEYSEP)[1];
}

function getKeyDef(fullKey) {
  return fullKey.split(ns.KEYDEFANDVALUEDEFSEP)[0];
}

function getValueDef(fullKey) {
  return getNamespace(fullKey).split(ns.KEYDEFANDVALUEDEFSEP)[1];
}

function getSubKeys(fullKey) {
  return getKey(fullKey).split(ns.SUBKEYSSEP).map(elem => decode(elem));
}
// Redis fullKey = keyNamespace + '-' + key
// Redis keyNamespace = keyDefinition + '|' + valueDefinition
// Redis key = subKey1 + ':' + subKey2 + ':' + subKey3

const redisKeyDefs = Object.keys(config.redisConfig.keys);

// Add config.redisConfig keys to ns object
redisKeyDefs.reduce((accu, keyDef) => {
  // First add the keyDef to ns object
  accu[keyDef.toUpperCase()] = keyDef;
  // Then add key namespace to ns object
  for (const valueDef of config.redisConfig.keys[keyDef].valueDefs) {
    accu[''.concat(keyDef.toUpperCase(), ns.NSVARSEP, valueDef.toUpperCase())] = joinNamespace(keyDef, valueDef);
  }
  return accu;
}, ns);

// Add config.redisConfig constants to ns object
if ('constants' in config.redisConfig) for (const constant of config.redisConfig.constants) ns[constant.toUpperCase()] = constant;

function getFullKeyParts(fullKey, ...subKeyNames) {
  try {
    const keyDef = getKeyDef(fullKey);
    const subKeys = getSubKeys(fullKey);

    return subKeyNames.map((subKeyName) => {
      if (subKeyName === ns.NAMESPACE) {
        return getNamespace(fullKey);
      } else if (subKeyName === ns.KEYDEF) {
        return keyDef;
      } else if (subKeyName === ns.VALUEDEF) {
        return getValueDef(fullKey);
      } else if (subKeyName === ns.KEY) {
        return getKey(fullKey);
      }
      const indexOfSubKey = config.redisConfig.keys[keyDef].subKeyDefs.indexOf(subKeyName);
      if (indexOfSubKey !== -1) return subKeys[indexOfSubKey];
      throw new Error(`cannot find the subkey ${subKeyName}`);
    });
  } catch (error) {
    logError('getFullKeyParts(): %o', error);
    throw error;
  }
}


function getKeyParts(keyDef, key, ...partsNames) {
  try {
    const fullKey = joinFullKey(joinNamespace(keyDef), key);
    return getFullKeyParts(fullKey, ...partsNames);
  } catch (error) {
    logError('getKeyParts(): %o', error);
    throw error;
  }
}

const redisTools = {
  joinNamespace,
  joinFullKey,
  join,
  joinSubKeys,
  getFullKeyParts,
  getKeyParts,
  getSubKeys,
  getNamespace,
  getKey,
  getKeyDef,
};

const redisBase = createRedis.createClient({ port: config.redisConfig.port });
export const redis = Object.assign(redisBase, redisTools, ns);
export const redisSub = Object.assign(redis.duplicate(), redisTools, ns);
debug('redis.ns %o', ns);
