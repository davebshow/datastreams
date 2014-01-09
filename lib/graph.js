fs = require('fs')

function Graph() {
	this.graph = {};

	this.createNode = function(node, attrs) {
		if (attrs) {
			this.graph[node] = attrs;
			this.graph[node].edges = [];
		} else {
			this.graph[node] = {edges: []};

		}
	};
}

Graph.prototype.saveGraph = function(fileName) {
	var graphString = JSON.stringify(this.graph);
	fs.writeFile(fileName, graphString);
}



exports.Graph = Graph