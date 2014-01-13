var levelup = require('levelup')
,   streams = require('./streams')
,   db

function DataStreams(fileName) {    
    // this need calback
    if (!(this instanceof DataStreams)) 
        return new DataStreams(fileName)
    db = levelup(fileName, { valueEncoding: 'json' });
    this.db = db // this is just for testing    
}

DataStreams.prototype.addObject = function(type, unique, options) {
    // this will be depricated I'd imagine
    var node = type + '!' + unique
    console.log('node', node)
    db.put(node, options, function (err) {
        if (err) return console.log('Ooops!', err)
    })
}

DataStreams.prototype.stream = function(opt) {
    return db.createReadStream(opt)
}

DataStreams.prototype.createStream = function(arrayOfObjects, callback) {
    var endpoint = arrayOfObjects[arrayOfObjects.length - 1]
    ,   stream = buildStream(arrayOfObjects)
    if (callback) callback(new DataStream(stream))
    return new DataStream(stream, endpoint) 
}


// Data stream functin object and methods
function DataStream(stream, endpoint) {
    if (!(this instanceof DataStream)) 
        return new DataStream(stream, endpoint)

    this.stream = stream
    this.endpoint = endpoint
}

DataStream.prototype.addData = function(components, endData) {
    // return and callback ? yess
    var uniques = buildStream(components)
    ,   stream = this.stream + uniques
    db.put(stream, endData, function (err) {
        if (err) return console.log('Ooops!', err)
    })
}

DataStream.prototype.addStreamingData = function() {
    console.log('adding streaming data')
}

DataStream.prototype.simpleStream = function(start, end) {
    // callback Maybe build this optionals different
    var newStreamArray = []
    ,   streamStart = this.stream
    ,   streamEnd = streamEnd = this.stream + '\xff'
    if (start) streamStart = this.stream + start
    if (end) streamEnd = this.stream + '\xff'
    return db.createReadStream({start: streamStart, end: streamEnd})   
}

DataStream.prototype.linkedStreamToObject = function(start, end, callback) {
    // this will need a return and callback - set up options here with default (everywher)
    var newStreamArray = []
    ,   streamStart = this.stream
    ,   streamEnd = streamEnd = this.stream + '\xff'
    ,   endpoint = this.endpoint
    if (start) streamStart = this.stream + start
    if (end) streamEnd = this.stream + '\xff'
    var source = db.createReadStream({start: streamStart, end: streamEnd})
    
    return new streams.LinkedStreamToObject(db, source, endpoint)       
}

DataStream.prototype.linkedStreamToStream = function(dataStream, start, end) {
    var streamStart = this.stream
    ,   streamEnd = streamEnd = this.stream + '\xff'
    ,   stream = dataStream.stream
    if (start) streamStart = this.stream + start
    if (end) streamEnd = this.stream + end + '\xff'
    var source = db.createReadStream({start: streamStart, end: streamEnd})
    return new streams.LinkedStreamToStream(db, source, stream)

}


function buildStream(arrayOfObjects) {
    if(typeof arrayOfObjects == 'string') return arrayOfObjects + '!'
    var stream = ''
    for (var i in arrayOfObjects) {
        var key = arrayOfObjects[i]
        stream += key + '!'
    }
    return stream

}



exports.DataStreams = DataStreams