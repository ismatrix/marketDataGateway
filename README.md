# marketDataGateway
This is the unified gateway to smartwin Market Data providers.

## Install
```
npm i -g pm2
npm install
```

## Prod
```
NODE_ENV=production DEBUG_FD=1 DEBUG=*,-babel DEBUG_COLORS=true pm2 start src/index.js --log-date-format="MM-DD HH:mm:ss" --name marketDataGateway -- --credentials-name markets.quantowin.com
```

## 重启
```
pm2 restart marketDataGateway
```

## 日志
```
pm2 logs marketDataGateway
~/.pm2/logs  # 日志文件路径
```

## 架构介绍

marketDataGateway启起来后要做这些事情:
* 链后台行情服务 (ice: ip+端口)
* 对外提供获取行情 (grpc接口)
