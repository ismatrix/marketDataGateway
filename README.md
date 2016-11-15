# marketDataGateway
This is the unified gateway to smartwin Market Data providers.
## Install
```
npm i -g pm2
npm install
```

## Dev
```
DEBUG=*,-babel DEBUG_COLORS=true pm2 start src/index.js --watch --no-autorestart --name marketDataGateway  -- --credentials-name invesmart.win
pm2 logs marketDataGateway
```

## Prod
```
npm run compile
DEBUG=*,-babel DEBUG_COLORS=true pm2 start dist/app.js --name marketDataGateway -- --credentials-name invesmart.win
```
