var levelup = require('levelup')
,   streams = require('./streams')
,   db


function UserException(message) {
   this.message = message;
   this.name = "UserException";
}

function DataStreams(fileName) {    
    // this need calback
    if (!(this instanceof DataStreams)) 
        return new DataStreams(fileName)
    db = levelup(fileName, { valueEncoding: 'json' });
    this.db = db // this is just for testing    
}

DataStreams.prototype.addObject = function(key, attrs, opts, cb) {
    // this will be depricated I'd imagine
    if (typeof opts === 'function') {
        cb = opts
        opts = undefined
    }
    var unique = attrs.unique
    if (typeof unique === 'undefined') throw new UserException('Object must have key "unique" in attrs')
    ,   node = key + '!' + unique
    db.put(node, opts, function (err) {
        if (err) return console.log('Ooops!', err)
    })
}

DataStreams.prototype.stream = function(opts) {
    return db.createReadStream(opt)
}

DataStreams.prototype.createStream = function(keyArray, cb) {
    var lastKey = keyArray[keyArray.length - 1]
    ,   stream = buildStream(keyArray)
    if (cb) cb(null, new DataStream(stream))
    return new DataStream(stream, lastKey) 
}


// Data stream functin object and methods
function DataStream(stream, lastKey) {
    if (!(this instanceof DataStream)) 
        return new DataStream(stream, lastKey)
    this.stream = stream
    this.lastKey = lastKey
}

DataStream.prototype.addData = function(keys, attrs, cb) {
    // return and cb ? yess
    var uniques = buildStream(keys)
    ,   stream = this.stream + uniques
    db.put(stream, attrs, function (err) {
        if (err) return console.log('Ooops!', err)
        if (cb) cb(null)

    })
}

DataStream.prototype.addStreamingData = function() {
    console.log('adding streaming data')
}

DataStream.prototype.simpleStream = function(opts, cb) {
    // cb Maybe build this optionals different
    if (typeof opts === 'function') {
        cb = opts
        opts = undefined
    }
    var streamStart = this.stream
    ,   streamEnd = streamEnd = this.stream + '\xff'
    if (opts) {
        if (opts.start) streamStart = this.stream + start
        if (opts.end) streamEnd = this.stream + '\xff'
    }
    if (cb) cb(null, db.createReadStream({start: streamStart, end: streamEnd}))
    return db.createReadStream({start: streamStart, end: streamEnd})   
}

DataStream.prototype.linkedStreamToObject = function(opts, cb) {
    // this will need a return and cb - set up options here with default (everywher)
    if (typeof opts === 'function') {
        cb = opts
        opts = undefined
    }
    var newStreamArray = []
    ,   streamStart = this.stream
    ,   streamEnd = streamEnd = this.stream + '\xff'
    ,   lastKey = this.lastKey
    if (opts) {
        if (opts.start) streamStart = this.stream + start
        if (opts.end) streamEnd = this.stream + '\xff'
    }
    var source = db.createReadStream({start: streamStart, end: streamEnd})
    if (cb) cb(null, new streams.LinkedStreamToObject(db, source, lastKey))
    return new streams.LinkedStreamToObject(db, source, lastKey)       
}

DataStream.prototype.linkedStreamToStream = function(dataStream, opts) {
    var streamStart = this.stream
    ,   streamEnd = streamEnd = this.stream + '\xff'
    ,   stream = dataStream.stream
    if (opts) {
        if (opts.start) streamStart = this.stream + opts.start
        if (opts.end) streamEnd = this.stream + '\xff'
    }
    var source = db.createReadStream({start: streamStart, end: streamEnd})
    return new streams.LinkedStreamToStream(db, source, stream)

}


function buildStream(keyArray) {
    if(typeof keyArray == 'string') return keyArray + '!'
    var stream = ''
    for (var i in keyArray) {
        var key = keyArray[i]
        stream += key + '!'
    }
    return stream

}



exports.DataStreams = DataStreams