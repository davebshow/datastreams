http = require('http');

dataStreams = require('./lib/streams');
ds = dataStreams.DataStreams('/tmp/test2.db');

ds.addNode('store', 'Brix', {unique: 'Brix', extra: 'this worked'})
ds.addNode('store', 'Comp', {unique: 'Comp', extra: 'twice'})
// Imperative paradigm
var stream1 = ['zip', 'store']

var zipStoreStream = ds.createStream(stream1);
zipStoreStream.addData(['52245', 'Brix'], {unique:'Brix'})
zipStoreStream.addData(['52240', 'Comp'], {unique: 'Comp'})
zipStoreStream.readStream();

var stream2 = ['store', 'wine']

var storeWineStream = ds.createStream(stream2);
storeWineStream.addData(['Brix', 'Red'], {unique: 'Red', owner: 'Nick'})
storeWineStream.addData(['Comp', 'White'], {unique: 'White', owner: 'Guy'})
//storeWineStream.readStream('Comp');

// using the callbacks
var stream3 = ['wine', 'winery']
ds.createStream(stream3, function(stream) {
	stream.addData(['Red', 'Hinman'], {unique: 'Hinman', place: 'Oregon'})
	//stream.readStream()
});


zipStoreStream.joinStream(storeWineStream, '52245');

var db = ds.db;



    // read the whole store as a stream and print each entry to stdout
    //db.createReadStream()
    //  .on('data', console.log)
    //  .on('close', function () {
    //    db.close()
    //  })



var server = http.createServer(function(req, res) {
	res.writeHead(200);
	res.end(JSON.stringify(g));
});

server.listen(3000);
