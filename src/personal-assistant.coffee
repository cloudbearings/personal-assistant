async = require 'async'
Q = require 'q'
winston = require 'winston'
Pusher = require 'pusher'
_ = require 'underscore'
util = require 'util'


MAX_CHUNK_SIZE = 50

PAFactory = (options) ->

    globalMiddleware = []

    # PAs for each namespace
    pas = {}
    rootPa = null

    heartbeatInterval = options.heartbeatInterval or 60 * 60 * 1000 # 1 hr
    redisPub = options.redisPub
    redisSub = options.redisSub
    redisClient = options.redisClient

    identifier = options.identifier

    handleUpdates = options.handleUpdates or false

    maxChunkSize = options.maxChunkSize or MAX_CHUNK_SIZE

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
                pa:socket:namespace:`socketId`           - a set of queryIds for the given socket
                pa:qryIds:namespace:`qryName`            - a set of queryIds for the given queryname
                pa:latestQueryVersion                    - a key holding the latest query version (to use when
                                                           determining whether a query result should be updated)
            ###
            @qryIdKeyPref = "pa:query:"
            if namespace then @qryIdKeyPref += namespace + ":"
            @socketIdKeyPref = "pa:socket:"
            if namespace then @socketIdKeyPref += namespace + ":"
            @qryNameListKeyPref = "pa:qryIds:"
            if namespace then @qryNameListKeyPref += namespace + ":"

            @latestQueryVersionKey = "pa:latestQueryVersion"

            # Do we want to listen for the maybeModifiedQuery (i.e. are we handling updates?)
            if handleUpdates
                channel = "maybeModifiedQuery" + if namespace then ":" + namespace else ""
                redisSub.subscribe channel
                redisSub.on "message", (_channel, message) =>
                    if _channel is channel then @maybeModifiedQueryMsgReceived message

                setInterval @_keepSocketAlive.bind(@), heartbeatInterval

        ### API BITS ###
        query: (qryName, data, initialProps, callback) ->

            # We allow bulk performing of queries. In this case the qryName
            # will be an object of the form {qryName: data}
            if typeof qryName is 'object'
                callback = initialProps
                initialProps = data
                qryData = qryName
                qryName = undefined
                data = undefined
                multiFormat = true
            else
                qryData = {}
                qryData[qryName] = data
                multiFormat = false


            if typeof initialProps is 'function'
                callback = initialProps
                initialProps = null

            initialProps or= {}

            qry = {
                qryData
            }

            externalProps = _.extend {}, initialProps

            @runQuery qry, externalProps, (err, result) ->
                if err then return callback? err
                # If we only have one result the transform the results
                qryIds = _.keys result
                if qryIds.length is 1 and not multiFormat
                    qryId = qryIds[0]
                    callback? null, qryId, result[qryId].result
                else
                    resultObj = {}
                    for qryId, res of result
                        resultObj[qryId] = res.result
                    callback? null, resultObj

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

        maybeModifiedQuery: (qryName, socketId, noSelfNotify) ->
            channel = "maybeModifiedQuery" + if @namespace then ":" + @namespace else ""
            redisClient.incr @latestQueryVersionKey, (err, version) =>
                if err then return winston.log err

                # If we have the socketId and should be self notifying then run all the self notification
                # stuff now. If we aren't self notifying then just pass the socketId through
                if noSelfNotify
                    redisPub.publish channel, JSON.stringify { qryName, version, socketId }
                else
                    redisPub.publish channel, JSON.stringify { qryName, version }
                    # Self notify if we have a socket id
                    if socketId
                        @notifySingleSocket qryName, socketId, version


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
            { qryName, version, socketId } = JSON.parse message
            # Get all queryIds from the queryname
            promise = Q.ninvoke redisClient, 'smembers', @qryNameListKeyPref + qryName
            promise.then (qryIds) =>
                # There could be lots of query ids here - we want to smooth out the
                # tasks so that we dont KO the server at 100% CPU. Use async.queue
                # which seems to do what we want
                recheckQ = async.queue(((qryId, callback) =>
                    @recheckQuery qryId, qryName, version, socketId, callback
                ), maxChunkSize)
                recheckQ.push qryIds, (err) ->
                    if err then winston.error err.stack, err

        recheckQuery: (qryId, qryName, version, socketId, callback) ->
            # Construct the qry object
            qry = {
                version
                qryId
                qryName
            }

            @runQuery qry, (err, resultObj) ->
                if err then return callback err

                # If the query has been modified then notify pusher that it has happened
                for qryId, resultData of resultObj
                    {modified, result} = resultData
                    if modified
                        channelName = ""
                        if identifier
                            channelName += identifier + '-'
                        channelName += "query-#{qryId}"
                        pusher.trigger channelName, 'modified query', result, socketId
                callback()

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

        # qryData is an object mapping query names
        # to query objects
        _getQryIdsForQuery: (externalProps, qryData) ->
            qryIdToNameMap = {}
            qryIdToQryMap = {}
            allIds = []
            for qryName, qry of qryData
                handler = @getHandler('uniqifier', qryName)

                unless handler? then throw new Error "Couldnt find a uniqifier for query name: #{qryName}"

                _ids = handler externalProps, qry

                unless _ids? then throw new Error "Couldnt get query id(s)"

                # The uniqifier could return multiple ids or a single one. We handle both here
                if Array.isArray _ids
                    # Lets just do some consistency checking...
                    unless Array.isArray(qry) and _ids.length is qry.length
                        throw new Error "The uniqifier has indicated multiple queries but they don't match up with the qry data passed in: #{utils.inspect qry}"
                    _ids.forEach (id, index) ->
                        qryIdToQryMap[id] = qry[index]
                        qryIdToNameMap[id] = qryName
                        allIds.push id
                else
                    qryIdToNameMap[_ids] = qryName
                    qryIdToQryMap[_ids] = qry
                    allIds.push _ids

            return {
                qryIdToNameMap
                qryIdToQryMap
                qryIdToResultMap: {}
                qryIdToExistingResultMap: {}

                allIds
            }

        # qryIdData is an object with keys of qryName and vals of the qryIds
        _getHandlersForQryIds: (qryIdData) ->
            knownHandlers = []
            handlerQryIdMap = []
            # We cant use functions as keys of an object so lets stick them
            # in an array and use the index as the key to map to the list of qryIds
            # for that handler
            for qryName, qryIds of qryIdData
                handler = @getHandler 'handler', qryName
                unless handler then throw new Error "Can't get handler for query name: #{qryName}"

                # Do we know about the handler?
                handlerId = knownHandlers.indexOf handler
                if handlerId isnt -1
                    ids = handlerQryIdMap[handlerId]
                    for id in qryIds
                        if id not in ids then ids.push id
                else
                    knownHandlers.push handler
                    handlerId = knownHandlers.indexOf handler
                    handlerQryIdMap[handlerId] = qryIds[..]

            return {
                handlers: knownHandlers
                qryIdsMap: handlerQryIdMap
            }

        # Do initial setup getting and mapping ids etc.
        _initializeQueryProps: (qry, externalProps) ->
            # There are two input options here - we've either been passed queryData
            # in which case we are doing a new query or we've been passed a queryName and qryId
            # in which case we are repeating an existing query.
            if qry.qryData
                qry.qryIdData = @_getQryIdsForQuery externalProps, qry.qryData
            else
                unless qry.qryId? 
                    throw new Error "Not enough data to construct query - #{util.inspect qry}"
                qry.qryIdData = {
                    qryIdToNameMap: {}
                    qryIdToQryMap: {}
                    qryIdToResultMap: {}
                    qryIdToExistingResultMap: {}

                    allIds: [qry.qryId]
                }
                if qry.qryName?
                    qry.qryIdData.qryIdToNameMap[qry.qryId] = [qry.qryName]
            return

        _findExistingQueries: (qryIds) ->
            promises = []
            for qryId in qryIds
                promises.push Q.ninvoke redisClient, 'hgetall', @qryIdKeyPref + qryId

            # Wait for all promises to complete
            return Q.all promises

        _removeQryIdFromData: (data, qryId) ->
            delete data.qryIdToNameMap[qryId]
            delete data.qryIdToQryMap[qryId]
            delete data.qryIdToResultMap[qryId]
            delete data.qryIdToExistingResultMap[qryId]
            data.allIds = _.without data.allIds, qryId
            return

        # Loop through all the existing queries and 
        # 1. If we haven't already got data for the query build it from
        #    the existing option
        # 2. If we do have data for the query then check that it matches
        #    the existing data.
        #    
        _buildAndCheckQueryFromExisting: (qryIdData, existingObjs, version) ->
            existingQueries = []
            existingObjs.forEach (existing, index) =>
                qryId = qryIdData.allIds[index]
                qryName = qryIdData.qryIdToNameMap[qryId]
                unless existing
                    existing = {}

                    # If we don't have an existing query then make sure its not in the set for the
                    # qryName by doing an SREM... (it will be put back in later if needed)
                    if qryName then redisClient.srem @qryNameListKeyPref + qryName, qryId

                # If we don't have an existing query and no actual query object (as could be the case
                # when doing a maybeModifiedQueries) then we dont want to continue any further. Return
                unless qryIdData.qryIdToQryMap[qryId] or existing.qry
                    @_removeQryIdFromData qryIdData, qryId
                    return

                _version = existing.version or null
                existingResult = if existing.result then JSON.parse existing.result else null
                
                qryIdData.qryIdToExistingResultMap[qryId] = existingResult

                if version? and _version? and version <= _version
                    qryIdData.qryIdToResultMap[qryId] = existingResult

                # If we have a query name check that this matches
                if qryName and existing.qryName
                    if String(qryName) isnt String existing.qryName 
                        # The query names don't match. Don't know how to continue - throw
                        # an error
                        throw new Error "Query names don't match for qryId: #{qryId}. Expected #{qryName} but got #{existing.qryName}"
                else if existing.qryName
                    qryName = existing.qryName
                    qryIdData.qryIdToNameMap[qryId] = qryName
                
                # We need a queryName to continue
                unless qryName then throw new Error "Have no query name for qryId: #{qryId}"

                # If there is an existing query check that it matches the one passed in
                existingQry = if existing.qry then JSON.parse existing.qry else null

                if existingQry? and not qryIdData.qryIdToQryMap[qryId]?
                    qryIdData.qryIdToQryMap[qryId] = existingQry
                else if existingQry? and qryIdData.qryIdToQryMap[qryId]?
                    # Check that the two match
                    if existing.qry isnt JSON.stringify(qryIdData.qryIdToQryMap[qryId])
                        throw new Error "Got two separate queries for qryId: #{qryId}\nExisting: #{utils.inspect existingQry}\nOriginal: #{utils.inspect qryIdData.qryIdToQryMap[qryId]}"
                return
            return

        _setQuerySession: (ids, externalProps, existingObjs) ->
            # We only allow the setting of query sessions from the existing one 
            # if there is a single queryId request
            if ids.length is 1
                externalProps.session = if existingObjs[0]?.session then  JSON.parse existingObjs[0].session else {}
            else
                externalProps.session = {}

        _runMiddleware: (externalProps, qryData) ->
            mwPromises = []
            middleware = []
            middleware.push globalMiddleware..., @middleware...
            middleware.forEach (mw) ->
                mwPromises.push Q.nfcall mw, externalProps, qryData

            # Now run through all the middleware
            return Q.all mwPromises

        # The queryIds must come in the same order as the results will...
        _handleOutstandingResultResponse: (qryIds, resultObj) ->
            return (results) ->
                unless Array.isArray qryIds
                    results = [results]
                    qryIds = [qryIds]

                results.forEach (result, index) ->
                    resultObj[qryIds[index]] = result

                return Q.resolve()

        _fetchOutstandingResults: (externalProps, qryIdData) ->
            # We need to get any query that we don't already
            # have a result for.
            outstandingQueries = _.reject qryIdData.allIds, (qryId) ->
                return qryIdData.qryIdToResultMap[qryId]?

            # Now work out which handlers we need
            if outstandingQueries.length > 0
                data = {}
                for qryId in outstandingQueries
                    qryName = qryIdData.qryIdToNameMap[qryId]
                    data[qryName] or= []
                    data[qryName].push qryId
                handlerData = @_getHandlersForQryIds data

                promises = []
                handlerData.handlers.forEach (handler, index) =>
                    qryIds = handlerData.qryIdsMap[index]
                    # Now we break the query out. If we have more than
                    # one construct an array. If we don't then keep as an
                    # individual
                    if qryIds.length is 1
                        qryId = qryIds[0]
                        _prom = Q.nfcall handler, externalProps, qryIdData.qryIdToQryMap[qryId]
                        _prom = _prom.then @_handleOutstandingResultResponse qryId, qryIdData.qryIdToResultMap
                        promises.push _prom
                    else if qryIds.length > 1
                        queries = []
                        for qryId in qryIds
                            queries.push qryIdData.qryIdToQryMap[qryId]

                        _prom = Q.nfcall handler, externalProps, queries
                        _prom = _prom.then @_handleOutstandingResultResponse qryIds, qryIdData.qryIdToResultMap
                        promises.push _prom
                promise = Q.all promises
            else
                promise = Q.resolve()

            return promise

        _storeResults: (qryIdData, version, session, socketId, haveOrigQry) ->
            # Now map each to an individual query
            promises = []
            _.each qryIdData.qryIdToResultMap, (qryResult, qryId) =>
                qryName = qryIdData.qryIdToNameMap[qryId]

                if version?
                    promise = Q.resolve version
                else
                    promise = Q.ninvoke redisClient, 'get', @latestQueryVersionKey

                promise = promise.then (_version) =>

                    # Store the query in redis (if it exists)
                    qryData = {
                        result: JSON.stringify qryResult
                        qryName: qryName
                        qry: JSON.stringify qryIdData.qryIdToQryMap[qryId]
                        version: String _version
                        session: JSON.stringify session
                    }
                    qryIdKey =  @qryIdKeyPref + qryId
                    promise = Q.ninvoke redisClient, 'hmset', qryIdKey, qryData

                    # If this is no TTL on this query then add one. Do this completely async since
                    # it shouldn't hold up getting the data out to the user
                    redisClient.ttl qryIdKey, (err, ttl) ->
                        if err then return winston.error err.stack, err
                        # If no timeout set or we've been passed in an original query then set an expires
                        # (the ttl check is more there as safety since the only way the ttl shouldnt exist
                        # is if we have an original query anyway)
                        if ttl is -1 or haveOrigQry
                            # -1 indicates no timeout set. So set one to 2.5 times the heartbeat interval
                            # (which is the interval that we check )
                            redisClient.pexpire qryIdKey, 2.5 * heartbeatInterval

                    promise = promise.then => 
                        promiseA = Q.ninvoke redisClient, 'sadd', @qryNameListKeyPref + qryName, qryId

                        # Store off the link from the socketId to the query...
                        if socketId
                            socketIdKey = @socketIdKeyPref + socketId
                            sockData = JSON.stringify {qryId: qryId, qryName: qryName}
                            promiseB = Q.ninvoke redisClient, 'sadd', socketIdKey, sockData

                            # Maybe set the ttl on the socketId key. Same as for the qryId key above
                            redisClient.ttl socketIdKey, (err, ttl) ->
                                if err then return winston.error err.stack, err
                                if ttl is -1 then redisClient.pexpire socketIdKey, 2.5 * heartbeatInterval
                                return
                        else
                            promiseB = Q.resolve()

                        return Q.all [promiseA, promiseB]
                    return promise
                promises.push promise
            return Q.all promises

        _createResultObject: (results, existingResults) ->
            resultObj = {}
            for qryId, result of results
                existing = existingResults[qryId]
                modified = JSON.stringify(result) isnt JSON.stringify(existing)
                resultObj[qryId] = {
                    result
                    modified
                }
            return resultObj


        runQuery: (qry, externalProps, callback) ->

            unless callback?
                callback = externalProps
                externalProps = {}

            # Initialize our parameters
            @_initializeQueryProps qry, externalProps
            
            # Find existing queries from all the ids we want
            qrypromise = @_findExistingQueries qry.qryIdData.allIds

            qrypromise = qrypromise.then (existingObjs) =>

                # Take the existing queries and construct the query data (or check that the query
                # data matches the existing data)
                @_buildAndCheckQueryFromExisting qry.qryIdData, existingObjs, qry.version

                @_setQuerySession qry.qryIdData.allIds, externalProps, existingObjs
                
                mwpromise = @_runMiddleware externalProps, qry.qryData
                
                # Go and fetch any otstanding results
                promise = mwpromise.then =>
                    return @_fetchOutstandingResults externalProps, qry.qryIdData

                return promise.then (qryResults) =>
                    # If we've been passed in a version then use that, otherwise
                    # get the latest version and use that
                    haveOrigQry = qry.qryData?
                    promise = @_storeResults qry.qryIdData, 
                                             qry.version, 
                                             externalProps.session, 
                                             externalProps.socketId, 
                                             haveOrigQry

                    return promise.then =>
                        resultObj = @_createResultObject qry.qryIdData.qryIdToResultMap,
                                                         qry.qryIdData.qryIdToExistingResultMap
                        return callback null, resultObj

            # Handle the error
            qrypromise.fail (err) ->
                winston.error err.stack
                return callback err

        notifySingleSocket: (qryName, socketId, version) ->
            # Get all the queries for the socket and then uncover the ones
            # we want for the given query name
            promise = Q.ninvoke redisClient, 'smembers', @socketIdKeyPref + socketId
            promise = promise.then (qryObjStrs) =>
                qryObjs = _.map qryObjStrs, (str) ->
                    return JSON.parse str

                cb = (err) ->
                    if err then winston.error err.stack, err
                    return

                # Recheck any queries that have the correct query name
                for obj in qryObjs
                    if obj.qryName is qryName
                        @recheckQuery obj.qryId, qryName, version, undefined, cb

            return promise
                

        _keepSocketAlive: ->

            channelOffset = identifier.length
            qryOffset = channelOffset + 7 # <identifier>-query-<qryId>
            socketOffset = channelOffset + 8 # <identifier>-socket-<socketId>

            getQryFromChannel = (channelName) ->
                return channelName[qryOffset..]

            getSocketFromChannel = (channelName) ->
                return channelName[socketOffset..]

            # Get all the active query channels from pusher and reset the expires value
            # on the related queries
            pusher.get { path: '/channels', params: {filter_by_prefix: "#{identifier}-query-"} }, (err, req, res) =>
                if err then return winston.warn err
                if res.statusCode is 200
                    result = JSON.parse res.body
                    channels = result.channels
                    for channelName, val of channels
                        qryId = getQryFromChannel channelName
                        # Set the expiry time - 1.5 heartbeats
                        redisClient.pexpire @qryIdKeyPref + qryId, 1.5 * heartbeatInterval

            # Get all the active sockets from pusher and reset the expires value
            pusher.get { path: '/channels', params: {filter_by_prefix: "#{identifier}-socket-"} }, (err, req, res) =>
                if err then return winston.warn err
                if res.statusCode is 200
                    result = JSON.parse res.body
                    channels = result.channels
                    for channelName, val of channels
                        sockId = getSocketFromChannel channelName
                        # Set the expiry time - 1.5 heartbeats
                        redisClient.pexpire @sockIdKeyPref + sockId, 1.5 * heartbeatInterval


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