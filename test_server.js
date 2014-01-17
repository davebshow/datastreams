http = require('http');

datastreams = require('./lib/datastreams');
ds = datastreams.DataStreams('/home/dbshow/level/test1.db');
//ds.stream().on('data', function(data) {console.log('stream', data)})

var storeStream = ds.createStream('store')
storeStream.addData('Brix', {unique: 'Brix', extra: 'this worked'})
storeStream.addData('Comp', {unique: 'Comp', extra: 'twice'})
// Imperative paradigm
var stream1 = ['zip', 'store']

var zipStoreStream = ds.createStream(stream1);
zipStoreStream.addData(['52245', 'Brix'], {unique:'Brix'})
zipStoreStream.addData(['52240', 'Comp'], {unique: 'Comp'})
zipStoreStream.linkedStreamToObject()

var stream2 = ['store', 'wine']

var storeWineStream = ds.createStream(stream2);
storeWineStream.addData(['Brix', 'Red'], {unique: 'Red', owner: 'Nick'})
storeWineStream.addData(['Comp', 'White'], {unique: 'White', owner: 'Guy'})
storeWineStream.simpleStream()

// using the callbacks
var stream3 = ['wine', 'winery']
ds.createStream(stream3, function(err, stream) {
    stream.addData(['Red', 'Hinman'], {unique: 'Hinman', place: 'Oregon'})
    //stream.linkedStream()
});

zipStoreStream.linkedStreamToStream(storeWineStream)
    .on('data', function(data) {console.log(data)})



var db = ds.db;



    // read the whole store as a stream and print each entry to stdout
    //db.createReadStream()
    //  .on('data', function(data) {console.log('read')})
    //  .on('close', function () {
    //db.close()
    //  })



var server = http.createServer(function(req, res) {
    res.writeHead(200);
    res.end(JSON.stringify(g));
});

server.listen(3000);
