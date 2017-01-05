# marketDataGateway
This is the unified gateway to smartwin Market Data providers.
## Install
```
npm i -g pm2
npm install
```

## Dev
```
NODE_ENV=development DEBUG_FD=1 DEBUG=*,-babel DEBUG_COLORS=true pm2 start src/index.js --watch --no-autorestart --log-date-format="MM-DD HH:mm:ss"  --name marketDataGateway --node-args="--inspect=9231" -- --credentials-name localhost
pm2 logs marketDataGateway
```

## Prod
```
npm run compile
NODE_ENV=production DEBUG_FD=1 DEBUG=*,-babel DEBUG_COLORS=true pm2 start dist/app.js --log-date-format="MM-DD HH:mm:ss" --name marketDataGateway --node-args="--inspect=9241" -- --credentials-name markets.invesmart.net
```
