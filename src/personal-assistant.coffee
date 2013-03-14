async = require 'async'
Q = require 'q'
winston = require 'winston'
Pusher = require 'pusher'
_ = require 'underscore'


MAX_CHUNK_SIZE = 50

PAFactory = (options) ->

    globalMiddleware = []

    # PAs for each namespace
    pas = {}
    rootPa = null

    heartbeatInterval = options.heartbeatInterval or 60 * 1000
    redisPub = options.redisPub
    redisSub = options.redisSub
    redisClient = options.redisClient

    handleUpdates = options.handleUpdates or false
    handleCleanup = options.handleCleanup or false

    maxChunkSize = options.maxChunkSize or MAX_CHUNK_SIZE

    if handleUpdates or handleCleanup
        pusher = options.pusherClient or new Pusher options.pusher

    ###*
     * API:
     * 
     * `query`       - Pass in query name and qry data. Return
     *                 the queryId and the result of the query
     * `extendQuery` - Pass in queryId and new query data. Return
     *                 the new queryId, the query result and the new
     *                 extended query
     * `maybeModifiedQuery` - pass in the queryname 
     *                 If anything has changed then we'll get a push out to 
     *                 all listeners that things have changed.
     * `setQueryUniqifier` - setup a handler to basically create the queryId
     *                  from the query.
     * `setQueryHandler` - register the function to handle a specific query name
     * `setQueryExtender` - register the function to handle extending a query
     *
     * `use`         - Add in middleware. Middleware should take the qry parameter
     *                 and a callback
     *
     * `qry` parameter
     *
     * Has the following attributes
     * - session (a session like object that gets stored alongside the query)
     * - qryName
     * - handler
     * - uniqifier
     * - version (optional version to perform the query at. Absent means whatever version)
     * - qryId (may not be present if we don't have one yet)
     * - qry
     * - result (may not be present if we dont have one yet)
     * - modified (has the query been modified this time)
     *
     * Plus middleware etc could add to this.
    ###
    class PersonalAssistant

        constructor: (namespace) ->
            if namespace
                pas[namespace] = this
            else
                rootPa = this

            @namespace = namespace
            @queryHandlers = {}
            @middleware = []

            ###
                We set up the following keys in redis
                pa:query:namespace:`qryId`               - the data for the queryId - { result, qryName, qry, version }
                pa:qryIds:namespace                      - all query Ids
                pa:qryIds:namespace:`qryName`            - a set of queryIds for the given queryname
                pa:latestQueryVersion                    - a key holding the latest query version (to use when
                                                           determining whether a query result should be updated)
            ###
            @qryIdKeyPref = "pa:query:"
            if namespace then @qryIdKeyPref += namespace + ":"
            @allQryIdsKey = "pa:qryIds"
            if namespace then @allQryIdsKey += ":" + namespace
            @qryNameListKeyPref = "pa:qryIds:"
            if namespace then @qryNameListKeyPref += namespace + ":"

            @latestQueryVersionKey = "pa:latestQueryVersion"

            # Do we want to listen for the maybeModifiedQuery (i.e. are we handling updates?)
            if handleUpdates
                channel = "maybeModifiedQuery" + if namespace then ":" + namespace else ""
                redisSub.subscribe channel
                redisSub.on "message", (_channel, message) =>
                    if _channel is channel then @maybeModifiedQueryMsgReceived message

        ### API BITS ###
        query: (qryName, data, initialProps, callback) ->

            if typeof initialProps is 'function'
                callback = initialProps
                initialProps = null

            initialProps or= {}

            _qry = {
                qryName
                qry: data
            }

            qry = _.extend {}, initialProps, _qry

            @runQuery qry, (err, qry) ->
                if err then return callback err
                callback? null, qry.qryId, qry.result

        extendQuery: (qryName, qryId, data, initialProps, callback) ->
            if typeof initialProps is 'function'
                callback = initialProps
                initialProps = null

            extender = @getHandler 'extender', qryName
            promise = @_extendQuery extender, qryId, data
            promise.then (newQry) =>
                @query qryName, newQry, initialProps, callback
            promise.fail (err) ->
                callback err

        maybeModifiedQuery: (qryName) ->
            channel = "maybeModifiedQuery" + if @namespace then ":" + @namespace else ""
            redisClient.incr @latestQueryVersionKey, (err, version) ->
                if err then return winston.log err
                redisPub.publish channel, JSON.stringify { qryName, version }

        setQueryUniqifier: (qryName, uniqifierFn, regex) ->
            opts = @queryHandlers[qryName] or= {}
            opts.uniqifier = uniqifierFn
            opts.regex = regex

        setQueryHandler: (qryName, handlerFn, regex) ->
            opts = @queryHandlers[qryName] or= {}
            opts.handler = handlerFn
            opts.regex = regex

        setQueryExtender: (qryName, extendFn, regex) ->
            opts = @queryHandlers[qryName] or= {}
            opts.extender = extendFn
            opts.regex = regex

        use: (_middleware) ->
            @middleware.push _middleware

        ### Internal utils ###

        getHandler: (handlerName, qryName) ->
            handlers = @queryHandlers[qryName]
            if handlers
                handler = handlers[handlerName]
            unless handler
                # Try regex
                for name, handlers of @queryHandlers
                    if handlers.regex
                        re = new RegExp(name)
                        if re.test qryName
                            handler = handlers[handlerName]
            return handler

        maybeModifiedQueryMsgReceived: (message) ->
            { qryName, version } = JSON.parse message

            # Get all queryIds from the queryname
            promise = Q.ninvoke redisClient, 'smembers', @qryNameListKeyPref + qryName
            promise.then (qryIds) =>
                # There could be lots of query ids here - we want to smooth out the
                # tasks so that we dont KO the server at 100% CPU. Use async.queue
                # which seems to do what we want
                recheckQ = async.queue(((qryId, callback) =>
                    @recheckQuery qryId, qryName, version, callback
                ), maxChunkSize)
                recheckQ.push qryIds, (err) ->
                    winston.error err.stack, err

        recheckQuery: (qryId, qryName, version, callback) ->
            # Construct the qry object
            qry = {
                version
                qryId
            }

            @runQuery qry, (err, qry) ->
                if err then return callback err

                # If the query has been modified then notify pusher that it has happened
                if qry.modified
                    pusher.trigger "query-#{qry.qryId}", 'modified query', qry.result

         _extendQuery: (extender, qryId, qry) ->
            unless extender then return Q.fcall -> throw new Error "No extender for " + qryId
            # 1. Get the existing query
            # 2. Call the extender 
            promise = Q.ninvoke redisClient, 'hget', @qryIdKeyPref + qryId, 'qry'
            promise = promise.then (existingQryStr) ->
                unless existingQryStr then return Q.fcall -> throw new Error "No existing query with id: " + qryId
                existingQry = JSON.parse existingQryStr
                return Q.nfcall extender, existingQry, qry
            return promise

        runQuery: (qry, callback) ->

            # Go and see if there is an existing query
            qrypromise = Q.fcall =>
                unless qry.qryId 
                    if qry.uniqifier
                        qry.qryId = qry.uniqifier qry
                    else
                        qry.qryId = @getHandler('uniqifier', qry.qryName)? qry

                unless qry.qryId then return callback new Error "Couldnt get a query id"
                return

            qrypromise = qrypromise.then => 
                Q.ninvoke redisClient, 'hgetall', @qryIdKeyPref + qry.qryId
            qrypromise = qrypromise.then (existing) =>
                existing or= {}

                _version = existing.version or null
                existingQry = existing.result or null

                if existing.qryName then qry.qryName = existing.qryName 
                if existing.qry then qry.qry = JSON.parse existing.qry

                # Store the handler
                unless qry.handler
                    qry.handler = @getHandler 'handler', qry.qryName

                unless qry.handler then return callback new Error "Couldn't get a handler for the query with name: #{qry.qryName}"

                qry.session = if existing.session then  JSON.parse existing.session else {}
                
                mwPromises = []
                middleware = []
                middleware.push globalMiddleware..., @middleware...
                middleware.forEach (mw) ->
                    mwPromises.push Q.nfcall mw, qry

                # Now run through all the middleware
                promise = Q.all mwPromises

                promise = promise.then ->
                    # If we have an existing query which is at a version greater than or
                    # equal to the version we are after then just return a promise
                    # with that value (we need to convert it to JSON though)
                    if existingQry? and (qry.version and qry.version <= _version)
                        qrypromise = Q.fcall -> JSON.parse existingQry
                    else
                        qrypromise = Q.nfcall qry.handler, qry
                    return qrypromise

                return promise.then (qryResult) =>
                    # If we've been passed in a version then use that, otherwise
                    # get the latest version and use that
                    if qryResult
                        qry.result = qryResult
                        if qry.version?
                            promise = Q.resolve qry.version
                        else
                            promise = Q.ninvoke redisClient, 'get', @latestQueryVersionKey
                        promise = promise.then (_version) =>
                            qry.version = _version
                            # Store the query in redis (if it exists)
                            qryData = {
                                result: JSON.stringify qry.result
                                qryName: qry.qryName
                                qry: JSON.stringify qry.qry
                                version: String _version
                                session: JSON.stringify qry.session
                            }
                            promise = Q.ninvoke redisClient, 'hmset', @qryIdKeyPref + qry.qryId, qryData
                            promise = promise.then => 
                                Q.ninvoke redisClient, 'sadd', @qryNameListKeyPref + qry.qryName, qry.qryId

                            return promise
                    else
                        promise = Q.resolve()

                    return promise.then ->
                        # Return the response to the client
                        modified = qryResult != existingQry
                        qry.modified = modified
                        return callback null, qry

            # Handle the error
            qrypromise.fail (err) ->
                winston.error err.stack
                return callback err

    PAConstructor = (namespace) ->
        # Return either the existing PA for this namespace or create
        # a new one
        if (namespace? and pas[namespace]?)
            return pas[namespace]
        else if (not namespace? and rootPa?)
            return rootPa
        else
            return new PersonalAssistant namespace


    PAConstructor.use = (_middleware) ->
        globalMiddleware.push _middleware

    return PAConstructor


module.exports.PAFactory = (args...) -> return new PAFactory args...