# marketDataGateway
This is the unified gateway to smartwin Market Data providers.

<p align="center">
    <img src ="https://img.shields.io/badge/version-3.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-windows|linux|macos-yellow.svg"/>
    <img src ="https://img.shields.io/badge/nodejs-6.0+-blue.svg" />
    <img src ="https://img.shields.io/github/workflow/status/vnpy/vnpy/Python%20application/master"/>
    <img src ="https://img.shields.io/github/license/vnpy/vnpy.svg?color=orange"/>
</p>

商品期货行情服务，上游通过 zeroice 接入 [china-future-exchange-ctp]， 通过[gRPC]对外提供实时行情、历史行情服务

## Install
```
npm i -g pm2
npm install
```

## Prod

* 配置文件 ./src/config.js
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
