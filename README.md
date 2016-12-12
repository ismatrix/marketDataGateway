# marketDataGateway
This is the unified gateway to smartwin Market Data providers.
## Install
```
npm i -g pm2
npm install
```

## Dev
```
DEBUG_FD=1 DEBUG=*,-babel DEBUG_COLORS=true pm2 start src/index.js --watch --no-autorestart --log-date-format="MM-DD HH:mm:ss"  --name marketDataGateway -- --credentials-name markets.invesmart.net
pm2 logs marketDataGateway
```

## Prod
```
npm run compile
DEBUG_FD=1 DEBUG=*,-babel DEBUG_COLORS=true pm2 start dist/app.js --log-date-format="MM-DD HH:mm:ss" --name marketDataGateway -- --credentials-name markets.invesmart.net
```
