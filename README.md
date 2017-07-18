# marketDataGateway
This is the unified gateway to smartwin Market Data providers.

## Install
```
npm i -g pm2
npm install
```

## Dev
```
NODE_ENV=development DEBUG_FD=1 DEBUG=*,-babel DEBUG_COLORS=true pm2 start src/index.js --watch src --no-autorestart --log-date-format="MM-DD HH:mm:ss"  --name marketDataGateway --node-args="--inspect=9231" -- --credentials-name localhost
pm2 logs marketDataGateway
```

## Prod
```
npm run compile
NODE_ENV=production DEBUG_FD=1 DEBUG=*,-babel DEBUG_COLORS=true pm2 start dist/app.js --log-date-format="MM-DD HH:mm:ss" --name marketDataGateway -- --credentials-name markets.invesmart.net
```

## 重启
```
pm2 restart marketDataGateway
```

## 架构介绍

marketDataGateway启起来后要做这些事情:
* 链后台行情服务 (ice: ip+端口)
* 对外提供获取行情 (grpc接口)
