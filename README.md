personal-assistant
==================

A distributed websocket based mechanism for managing queries and pushing updates when the queries change

```javascript
var PAFactory = require('personal-assistant');

var PersonalAssistant = PAFactory(options);

```

# Options #

* io - the socket.io server to use
* heartbeatInterval - interval in ms to use as a heartbeat. Defaults to 1min
* redisPub - a redis client to use to publish messages
* redisSub - a redis client to subscribe to messages
* redisClient - a redis client to use for other messages
* handleUpdates - should the PA handle looking for updates etc. Defaults to `false`
* handleCleanup - should the PA handle cleaning up dead resources? Defaults to `false`
* pusher - object with the `appId`, `key` and `secret` attributes
