redis = require 'redis'
{PAFactory} = require '../'
Q = require 'q'
winston = require 'winston'

# Remove winston console logging
winston.remove winston.transports.Console

rClient = redis.createClient()

module.exports.newPaFactory = (update) ->
    redisPub = redis.createClient()
    redisSub = redis.createClient()
    redisClient = redis.createClient()
    opts = {
        redisPub,
        redisSub,
        redisClient,
        handleUpdates: update
        pusherClient: Pusher
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

module.exports.cleanupKeys = (pa, callback) ->
    ns = pa.namespace
    promise = Q.ninvoke rClient, 'keys', "pa:query:#{ns}:*"
    promise = promise.then (keys) -> if keys.length then Q.ninvoke rClient, 'del', keys... else Q.resolve()
    promise = promise.then -> Q.ninvoke rClient, 'keys', "pa:qryIds:#{ns}:*"
    promise = promise.then (keys) -> if keys.length then Q.ninvoke rClient, 'del', keys... else Q.resolve()
    promise.then -> callback()
    promise.fail (err) -> callback(err)


module.exports.Pusher = Pusher = {
    trigger: ->
}

