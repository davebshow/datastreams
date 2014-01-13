var stream = require('stream')
,   util = require('util')
,   Readable = stream.Readable
,   Writable = stream.Writable


util.inherits(LinkedStreamToObject, Readable)
util.inherits(LinkedStreamToStream, Readable)


function LinkedStreamToStream(db, source, stream) {

    if (!(this instanceof LinkedStreamToStream))
        return new LinkedStreamToStream(source);

    Readable.call(this, {objectMode: true})

    this._db = db
    this._source = source  
    this._stream = stream 
}

LinkedStreamToStream.prototype._read = function(size) {
   
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
        data.value = self._db.createReadStream({start: start, end: end})
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

exports.LinkedStreamToObject = LinkedStreamToObject
exports.LinkedStreamToStream = LinkedStreamToStream