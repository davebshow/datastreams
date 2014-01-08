exports.Graph = function() {
	this.nodes = {};

	this.createNode = function(node, attrs) {
		if (attrs) {
			this.nodes[node] = attrs;
			this.nodes[node].edges = [];
		} else {
			this.nodes[node] = {edges: []};

		}
	};
}

