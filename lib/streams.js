var levelup = require('levelup')
var async = require('async')
// open a data store
var db

function DataStreams(fileName) {	
	// this need calback
	if (!(this instanceof arguments.callee)) {
    	return new DataStreams(fileName)
	} else {
		db = levelup(fileName, { valueEncoding: 'json' });
		this.db = db // this is just for testing
	}
}

DataStreams.prototype.addNode = function(type, unique, options) {
	var node = type + '!' + unique
	console.log('node', node)
	db.put(node, options, function(err) {
		if (err) return console.log('Ooops!', err)
	})
}

DataStreams.prototype.createStream = function(arrayOfObjects, callback) {
	var endpoint = arrayOfObjects[arrayOfObjects.length - 1]
	,   stream = buildStream(arrayOfObjects)
	if (callback) callback(new DataStream(stream))
	return new DataStream(stream, endpoint)	
}

function DataStream(stream, endpoint) {
	this.stream = stream
	this.endpoint = endpoint
}

DataStream.prototype.addData = function(components, endData) {
	// return and callback ? yess
	var uniques = buildStream(components)
	,	stream = this.stream + uniques
	db.put(stream, endData, function(err) {
		if (err) return console.log('Ooops!', err)
	})
}

DataStream.prototype.addStreamingData = function() {
	console.log('adding streaming data')
}

DataStream.prototype.readStream = function(start, end) {
	// this will need a return and callback
	var entries = []
	,   streamStart = this.stream
	,   streamEnd = streamEnd = this.stream + '\xff'
	,   endpoint = this.endpoint
	if (start) streamStart = this.stream + start
	if (end) streamEnd = this.stream + '\xff'
	db.createReadStream({start: streamStart, end: streamEnd})
		.on('data', function(data) {
			entries.push(data);
			var key = endpoint + '!' + data.value['unique']
			console.log('key', key)
			db.get(key, function (err, value) {
    			if (err) return console.log('Ooops!', err)
				console.log(value)
			})
		})
		.on('close', function() {console.log('closed stream')})
}

DataStream.prototype.joinStream = function(dataStream, start, end) {
	var newStarts = []
	,   streamStart = this.stream
	,   streamEnd = streamEnd = this.stream + '\xff'
	if (start) streamStart = this.stream + start
	if (end) streamEnd = this.stream + '\xff'
		db.createReadStream({start: streamStart, end: streamEnd})
			.on('data', function(data) {
				var start = dataStream.stream + data.value['unique']
				newStarts.push(start);	
			})
			.on('close', function() {
				async.map(newStarts, _readStream, function(err, results) {
					if (err) console.log('done')
					console.log(results)
				})
			})
}

function _readStream(start, callback) {
	var end = start + '\xff'
	, 	results = []
	db.createReadStream({start: start, end: end})
		.on('data', function(data) {results.push(data)})
		.on('close', function() {return callback(null, results[0])})
}

function buildStream(arrayOfObjects) {
	var stream = ''
	for (var i in arrayOfObjects) {
		var key = arrayOfObjects[i]
		stream += key + '!'
	}
	return stream
}

exports.DataStreams = DataStreams