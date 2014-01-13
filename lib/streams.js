var stream = require('stream')
,   util = require('util')
,   Readable = stream.Readable
,   Writable = stream.Writable
,   Transform = stream.Transform

util.inherits(ArrayStream, Readable)
util.inherits(StreamToStreamWrapper, Readable)
util.inherits(LinkedStreamToObject, Readable)
util.inherits(LinkedStreamToStream, Readable)

function ArrayStream(arr) {

    if (!(this instanceof ArrayStream))
        return new ArrayStream(arr);

    Readable.call(this, {objectMode: true})

    this._arr = arr
}

ArrayStream.prototype._read = function(size) {
    var self = this
    this._arr.forEach(function (element) {
        self.push(element)
    })
    self.push(null)
}

function StreamToStreamWrapper(source) {

    if (!(this instanceof StreamToStreamWrapper))
        return new StreamToStreamWrapper(source);

    Readable.call(this, {objectMode: true})

    this._source = source
    //this._initial = initial
    
}
StreamToStreamWrapper.prototype._read = function(size) {
    
    var self = this

    this._source.on('end', function() {
        console.log('StreamToStreamWrapper closed')
    })

    this._source.on('error', function (err) {
        console.log(err)
    })

    this._source.on('data', function (chunk) {
        //console.log('chunk', chunk)
        self.push(chunk)
    })
}

function LinkedStreamToStream(db, source, stream) {

    if (!(this instanceof LinkedStreamToStream))
        return new LinkedStreamToStream(source);

    Readable.call(this, {objectMode: true})

    this._db = db
    this._source = source  
    this._stream = stream 
}

LinkedStreamToStream.prototype._read = function(size) {
    console.log('protoreader')
    var self = this

    this._source.on('end', function() {
        console.log('StreamWrapper closed')
    })

    this._source.on('error', function (err) {
        console.log(err)
    })

    this._source.on('data', function (data) {
        var start = self._stream + data.value['unique']
        var end = start + '\xff'
        //console.log('S2S', data)
        var readable = self._db.createReadStream({start: start, end: end})
        data.value = new StreamToStreamWrapper(readable)
        self.push(data)
    
    })
}


function LinkedStreamToObject(db, source, endpoint) {

    if (!(this instanceof LinkedStreamToObject))
        return new LinkedStreamToObject(source);

    Readable.call(this, {objectMode: true})

    this._db = db
    this._source = source
    this._endpoint = endpoint
    
}

LinkedStreamToObject.prototype._read = function(size) {
    console.log('read')
    var self = this

    this._source.on('end', function() {
        console.log('LinkedStreamToObject closed')
    })

    this._source.on('error', function (err) {
        console.log(err)
    })

    this._source.on('data', function (data) {
        var key = self._endpoint + '!' + data.value['unique'] + '!'
        this._db.get(key, function (err, value) {
            if (err) return console.log('Ooops! Could not get endpoint object', err)
            data.value = value
            self.push(data)
        })
    })
}

exports.ArrayStream = ArrayStream
exports.StreamToStreamWrapper = StreamToStreamWrapper
exports.LinkedStreamToObject = LinkedStreamToObject
exports.LinkedStreamToStream = LinkedStreamToStream