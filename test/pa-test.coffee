setup = require './setup'

describe 'querying', ->

    pa = null
    before ->
        factory = setup.newPaFactory()
        pa = factory('/ns')
        pa.setQueryHandler 'testquery', setup.dummyQueryHandler
        pa.setQueryUniqifier 'testquery', setup.dummyQueryUniqifier
        pa.setQueryHandler 'errorquery', setup.dummyQueryHandlerError
        pa.setQueryUniqifier 'errorquery', setup.dummyQueryUniqifier
        pa.setQueryUniqifier 'erroruniqifying', setup.dummyQueryUniqifierError

    it 'should be possible to perform a query', (done) ->
        pa.query 'testquery', {test: 'data'}, (err, qryId, response) ->
            qryId.should.equal 'unique'
            response.should.equal 'test'
            done()

    it 'should report errors uniqifying', (done) ->
        pa.query 'erroruniqifying', {test: 'error'}, (err, qryId, response) ->
            err.should.have.property('message', 'unique error')
            done()

    it 'should report errors querying', (done) ->
        pa.query 'errorquery', {test: 'error'}, (err, qryId, response) ->
            err.should.have.property('message', 'test error')
            done()

    it 'should be possible to set a regex based handler', (done) ->
        pa.setQueryUniqifier 'test-1234', setup.dummyQueryUniqifier
        pa.setQueryHandler 'test-.*', setup.dummyQueryHandler, true
        pa.query 'test-1234', {test: 'regex'}, (err, qryId, response) ->
            qryId.should.equal 'unique'
            response.should.equal 'test'
            done()

describe 'extending', ->

    pa = null
    qryId = null
    before (done) ->
        factory = setup.newPaFactory()
        pa = factory('/ns')
        pa.setQueryHandler 'testquery', setup.dummyQueryHandler
        pa.setQueryUniqifier 'testquery', setup.dummyQueryUniqifier
        pa.query 'testquery', {test : 'data'}, (err, _qryId, response) ->
            qryId = _qryId
            done()

    it 'should be possible to extend a query', (done) ->
        pa.setQueryExtender 'testquery', (existingQry, toExtend, callback) ->
            existingQry.should.eql {test: 'data'}
            toExtend.should.eql {more: 'data'}
            callback null, {test: 'data', more: 'data'}

        pa.setQueryUniqifier 'testquery', (qry) ->
            if qry.qry.more then return 'more' else return 'unique'
        pa.setQueryHandler 'testquery', (qry, callback) ->
            qry.qry.should.eql {test: 'data', more: 'data'}
            callback null, 'extended response'


        pa.extendQuery 'testquery', qryId, {more: 'data'}, (err, extendedId, result) ->
            result.should.eql 'extended response'
            extendedId.should.eql 'more'
            done()

describe 'middleware', ->
    pa = null
    before ->
        factory = setup.newPaFactory()
        pa = factory('/ns')

    it 'should be called before the query', (done) ->
        pa.setQueryUniqifier 'testquery', setup.dummyQueryUniqifier
        pa.setQueryHandler 'testquery', (qry, callback) ->
            qry.should.have.property('test', 'value')
            callback null, 'tada'
        pa.use (qry, callback) ->
            qry.test = 'value'
            callback()

        pa.query 'testquery', {test: 'data'}, (err, qryId, response) ->
            qryId.should.equal 'unique'
            response.should.equal 'tada'
            done()

    it 'should be possible to set some initial query properties', (done) ->
        pa.setQueryUniqifier 'testquery', setup.dummyQueryUniqifier
        pa.setQueryHandler 'testquery', (qry, callback) ->
            qry.should.have.property('test', 'value')
            callback null, 'tada'
        pa.use (qry, callback) ->
            qry.test = 'value'
            qry.should.have.property('earlyProp', true)
            callback()

        pa.query 'testquery', {test: 'data'}, {earlyProp: true}, (err, qryId, response) ->
            qryId.should.equal 'unique'
            response.should.equal 'tada'
            done()


describe 'updates', ->

describe 'cleanup', ->