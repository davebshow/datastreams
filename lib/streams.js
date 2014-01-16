var stream = require('stream')
,   util = require('util')
,   Readable = stream.Readable
,   Writable = stream.Writable
,   Transform = stream.Transform


util.inherits(LinkedStreamToObject, Readable)
util.inherits(LinkedStreamToStream, Readable)
util.inherits(StreamToStreamTransform, Transform)


function LinkedStreamToStream(db, reader, stream) {

    if (!(this instanceof LinkedStreamToStream))
        return new LinkedStreamToStream(source);

    Readable.call(this, {objectMode: true})

    this._db = db
    this._reader = reader  
    this._stream = stream 
}

LinkedStreamToStream.prototype._read = function(size) {
   
    var self = this

    this._reader.on('end', function() {
        console.log('StreamWrapper closed')
    })

    this._reader.on('error', function (err) {
        console.log(err)
    })

    this._reader.on('data', function (chunk) {
        var start = self._stream + chunk.value['unique']
        var end = start + '\xff'
        chunk.value = self._db.createReadStream({start: start, end: end})
        self.push(chunk)
    
    })
}


function StreamToStreamTransform() {

    if (!(this instanceof StreamToStreamTransform))
        return new StreamToStreamTransform();

    Transform.call(this, {objectMode: true})

}

StreamToStreamTransform.prototype._transform = function(chunk, encoding, done) {
    // working on transform NEED ASYNC
    var parts = chunk.key.split('!') 
    ,   streamObj = arrayToObj(parts)
    console.log('streamObj', streamObj)
    chunk.value.on('data', function(data) {
        var newParts = data.key.split('!')
        ,   newStreamObj = arrayToObj(newParts)

        console.log('streamObj2', newStreamObj)
        for (key in newStreamObj) {
            streamObj[key] = newStreamObj[key]
        }
        console.log('finalobject', streamObj)

    })
    this.push(streamObj)
    done()

}


function arrayToObj(arr) {
    var streamObj = {}
    for (var i=0; i<arr.length-2; i++) {
        streamObj[arr[i]] = arr[i + 2]
    }
    return streamObj
}


function LinkedStreamToObject(db, reader, endpoint) {

    if (!(this instanceof LinkedStreamToObject))
        return new LinkedStreamToObject(reader);

    Readable.call(this, {objectMode: true})

    this._db = db
    this._reader = reader
    this._endpoint = endpoint
    
}

LinkedStreamToObject.prototype._read = function(size) {

    var self = this

    this._reader.on('end', function() {
        console.log('LinkedStreamToObject closed')
    })

    this._reader.on('error', function (err) {
        console.log(err)
    })

    this._reader.on('data', function (chunk) {
        var key = self._endpoint + '!' + chunk.value['unique'] + '!'
        this._db.get(key, function (err, value) {
            if (err) return console.log('Ooops! Could not get endpoint object', err)
            chunk.value = value
            self.push(chunk)
        })
    })
}

exports.LinkedStreamToObject = LinkedStreamToObject
exports.LinkedStreamToStream = LinkedStreamToStream
exports.StreamToStreamTransform = StreamToStreamTransform