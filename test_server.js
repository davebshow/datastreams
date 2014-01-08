http = require('http');
graph = require('./lib/graph')
var g = new graph.Graph()
g.createNode(1, {color: 'blue'});
console.log(g);
console.log(g.nodes);


var server = http.createServer(function(req, res) {
	res.writeHead(200);
	res.end(JSON.stringify(g));
});

server.listen(3000);
