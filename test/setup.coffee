redis = require 'redis'
{PAFactory} = require '../'

module.exports.newPaFactory = (update, cleanup) ->
    redisPub = redis.createClient()
    redisSub = redis.createClient()
    redisClient = redis.createClient()
    opts = {
        redisPub,
        redisSub,
        redisClient,
        handleUpdates: update
        handleCleanup: cleanup
        Pusher
    }

    factory = PAFactory opts
    return factory

module.exports.dummyQueryHandler = (qry, callback) ->
    callback null, 'test'

module.exports.dummyQueryUniqifier = (qry) ->
    return 'unique'

module.exports.dummyQueryHandlerError = (qry, callback) ->
    callback new Error 'test error'

module.exports.dummyQueryUniqifierError = (qry) ->
    throw new Error 'unique error'

Pusher = {}