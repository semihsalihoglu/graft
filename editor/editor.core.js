/*
 * Graph Editor is based on Directed Graph Editor by rkirsling http://bl.ocks.org/rkirsling/5001347.
 */

/*
 * Editor is a class that encapsulates the graph editing window
 * @param {container, [undirected]} options - Initialize editor with these options.
 * {options.container} - HTML element that contains the editor svg.
 * {options.undirected} - Indicate whether the graph is directed/undirected.
 * @constructor
 */
function Editor(options) {
    this.container = 'body';
    this.undirected = false;
    // Readonly editor does not let users
    // add new nodes/links.
    this.readonly = false;
    // Data for the graph nodes and edges.
    this.defaultColor = '#FFFDDB'
    this.nodes = [];
    this.links = [];
    this.messages = [];

    if (options) {
        this.container = options['container'] ? options['container'] : this.container;
        this.undirected = options['undirected'] === true;
        if (options['dblnode']) {
            this.dblnode = options['dblnode'];
        }
    }
    this.setSize();
    this.lastKeyDown = -1;
    this.init();
    this.buildSample();
}

/*
 * Build a sample graph with three nodes and two edges.
 */
Editor.prototype.buildSample = function() {
    this.empty();
    // Start with a sample graph.
    for(var i = 0; i < 3; i++) {
        this.addNode();
    }
    this.addEdge('1', '2');
    this.addEdge('2', '3');
    this.restart();
}

/*
 * Empties the graph by deleting all nodes and links.
 */
Editor.prototype.empty = function() {
    // NOTE : Don't use this.nodes = [] to empty the array
    // This creates a new reference and messes up this.force.nodes
    this.nodes.length = 0;
    this.links.length = 0;
    this.messages.length = 0;
    this.numNodes = 0;
    this.restart();
}

/*
 * Initializes the SVG elements, force layout and event bindings.
 */
Editor.prototype.init = function() {
    // Initializes the SVG elements.
    this.initElements();
    // Binds events and initializes variables used to track selected nodes/links.
    this.initEvents();
    // Line displayed when dragging an edge off a node
    this.drag_line = this.svg.append('svg:path')
                                 .attr('class', 'link dragline hidden')
                                 .attr('d', 'M0,0L0,0');
    // Handles to link and node element groups.
    this.path = this.svg.append('svg:g').selectAll('path'),
    this.circle = this.svg.append('svg:g').selectAll('g');
    // Initializes the force layout.
    this.initForce();
    this.restart();
}

/*
 * Updates the graph. Called internally on various events.
 * May be called from the client after updating graph properties.
 */
Editor.prototype.restart = function() {
    this.resizeForce();
    this.restartNodes();
    this.restartLinks();

    // Set the background to light gray if editor is readonly.
    this.svg.style('background-color', this.readonly ? '#f9f9f9' : '#ffffff');
    // Set the graph in motion
    this.force.start();
}

/*
 * Handles mousedown event.
 * Insert a new node if CTRL key is not pressed. Otherwise, drag the graph.
 */
Editor.prototype.mousedown = function() {
    if (this.readonly === true) {
        return;
    }
    this.svg.classed('active', true);
    if (d3.event.ctrlKey || this.mousedown_node || this.mousedown_link) {
        return;
    }
    // Insert new node at point.
    var point = d3.mouse(d3.event.target),
        node =  this.addNode();
    node.x = point[0];
    node.y = point[1];
    this.restart();
}

/*
 * Returns all the messages sent by node with the given id.
 * Output format: {receiverId: message}
 * @param {string} id
 */
Editor.prototype.getMessagesSentByNode = function(id) {
    var messagesSent = {};
    for (var i = 0; i < this.messages.length; i++) {
        var messageObj = this.messages[i];
        if (messageObj.sender.id === id) {
            messagesSent[messageObj.receiver.id] = messageObj.message;
        }
    }
    return messagesSent;
}

/*
 * Returns all the messages received by node with the given id.
 * Output format: {senderId: message}
 * @param {string} id
 */
Editor.prototype.getMessagesReceivedByNode = function(id) {
    var messagesReceived = {};
    for (var i = 0; i < this.messages.length; i++) {
        var messageObj = this.messages[i];
        if (messageObj.receiver.id === id) {
            messagesReceived[messageObj.sender.id] = messageObj.message;
        }
    }
    return messagesReceived;
}

/*
 * Returns the edge list.
 * Edge list is the representation of the graph as a list of edges.
 * An edge is represented as a vertex pair (u,v).
 */
Editor.prototype.getEdgeList = function() {
    edgeList = '';

    for (var i = 0; i < this.links.length; i++) {
        var sourceId = this.links[i].source.id;
        var targetId = this.links[i].target.id;

        // Right links are source->target.
        // Left links are target->source.
        if (this.links[i].right) {
            edgeList += sourceId + '\t' + targetId + '\n';
        } else {
            edgeList += targetId + '\t' + sourceId + '\n';
        }
    }
    return edgeList;
}

/*
 * Returns the adjacency list.
 * Adj list is the representation of the graph as a list of nodes adjacent to
 * each node.
 */
Editor.prototype.getAdjList = function() {
    adjList = '';

    for (var i = 0; i < this.nodes.length; i++) {
        var id = this.nodes[i].id;
        var nodes = [];
        for (var j = 0; j < this.links.length; j++) {
            var link = this.links[j];
            if ((link.left === true || this.undirected === true) && link.target.id === id) {
                nodes.push(link.source.id);
            }
            if ((link.right === true || this.undirected === true) && link.source.id === id) {
                nodes.push(link.target.id);
            }
        }
        if (nodes.length > 0) {
            adjList += id + '\t';
            for (var j = 0; j < nodes.length; j++) {
                adjList += nodes[j] + '\t';
            }
            adjList += i != this.nodes.length - 1 ? '\n' : '';
        }
    }

    return adjList;
}

/*
 * Returns the list of nodes along with their attributes.
 */
Editor.prototype.getNodeList  = function() {
    nodeList = '';
    for (var i = 0; i < this.nodes.length; i++){
        nodeList += this.nodes[i].id + '\t' + this.nodes[i].attrs.join(',');
        nodeList += (i != this.nodes.length - 1 ? '\n' : '');
    }
    return nodeList;
}

/*
 * Handle the mousemove event.
 * Updates the drag line if mouse is pressed at present.
 * Ignores otherwise.
 */
Editor.prototype.mousemove = function() {
    if (this.readonly) {
        return;
    }
    // This indicates if the mouse is pressed at present.
    if (!this.mousedown_node) {
        return;
    }
    // Update drag line.
    this.drag_line.attr('d', 'M' + this.mousedown_node.x + ',' +
        this.mousedown_node.y + 'L' + d3.mouse(this.svg[0][0])[0] + ',' +
        d3.mouse(this.svg[0][0])[1]
    );
    this.restart();
}

/*
 * Handles the mouseup event.
 */
Editor.prototype.mouseup = function() {
    if (this.mousedown_node) {
        // hide drag line
        this.drag_line
            .classed('hidden', true)
            .style('marker-end', '');
    }

    this.svg.classed('active', false);

    // Clear mouse event vars
    this.resetMouseVars();
}

/*
 * Handles keydown event.
 * If Key is Ctrl, drags the graph using the force layout.
 * If Key is 'L' or 'R' and link is selected, orients the link likewise.
 * If Key is 'R' and node is selected, marks the node as reflexive.
 * If Key is 'Backspace' or 'Delete', deletes the selected node or edge.
 */
Editor.prototype.keydown = function() {
    if (this.lastKeyDown !== -1) {
        return;
    }
    this.lastKeyDown = d3.event.keyCode;

    // Ctrl key was pressed
    if (d3.event.keyCode === 17) {
        this.circle.call(this.force.drag);
        this.svg.classed('ctrl', true);
    }

    if (!this.selected_node && !this.selected_link) {
        return;
    }

    switch (d3.event.keyCode) {
        case 8: // backspace
        case 46: // delete
            if (this.selected_node) {
                this.nodes.splice(this.nodes.indexOf(this.selected_node), 1);
                this.spliceLinksForNode(this.selected_node);
            } else if (this.selected_link) {
                this.links.splice(this.links.indexOf(this.selected_link), 1);
            }

            this.selected_link = null;
            this.selected_node = null;
            this.restart();
            break;
        case 66: // B
            if (this.selected_link) {
                // set link direction to both left and right
                this.selected_link.left = true;
                this.selected_link.right = true;
            }

            this.restart();
            break;
        case 76: // L
            if (this.selected_link) {
                // set link direction to left only
                this.selected_link.left = true;
                this.selected_link.right = false;
            }

            this.restart();
            break;
        case 82: // R
            if (this.selected_node) {
                // toggle node reflexivity
                this.selected_node.reflexive = !this.selected_node.reflexive;
            } else if (this.selected_link) {
                // set link direction to right only
                this.selected_link.left = false;
                this.selected_link.right = true;
            }

            this.restart();
            break;
    }
}

/*
 * Handles the keyup event.
 * Resets lastKeyDown to -1.
 * Also resets the drag event binding to null if the key released was Ctrl.
 */
Editor.prototype.keyup = function() {
    this.lastKeyDown = -1;

    // Ctrl
    if (d3.event.keyCode === 17) {
        this.circle
            .on('mousedown.drag', null)
            .on('touchstart.drag', null);
        this.svg.classed('ctrl', false);
    }
}

/*
 * Builds the graph from adj list by constructing the nodes and links arrays.
 * @param {object} adjList - Adjacency list of the graph. attrs and msgs are optional.
 * Format:
 * {
 *  nodeId: {
 *            neighbors : [adjId1, adjId2...],
 *            vertexValues : [attr1, attr2...],
 *            outgoingMessages : {
 *                    receiverId1: "message1",
 *                    receiverId2: "message2",
 *                    ...
 *                  }
 *          }
 * }
 */
Editor.prototype.buildGraphFromAdjList = function(adjList) {
    this.empty();
    // Scan every node in adj list to build the nodes array.
    for (var nodeId in adjList) {
        var node = this.getNodeWithId(nodeId);
        if (!node) {
            node = this.addNode(nodeId);
        }
        var adj = adjList[nodeId]['neighbors'];
        // For every node in the adj list of this node,
        // add the node to this.nodes and add the edge to this.links
        for (var i = 0; i < adj.length; i++) {
            var adjId = adj[i];
            var adjNode = this.getNodeWithId(adjId);
            if (!adjNode) {
                adjNode = this.addNode(adjId);
            }
            // Add the edge.
            this.addEdge(nodeId, adjId);
        }
    }
    this.updateGraphData(adjList);
    this.restart();
}

/*
 * Updates scenario properties - node attributes and messages from adj list.
 * @param {object} scenario - scenario has the same format as adjList above,
 * but with 'adj' ignored.
 * **NOTE**: This method assumes the same scenario structure,
 * only updates the node attributes and messages exchanged.
 */
Editor.prototype.updateGraphData = function(scenario) {
    // Scan every node in adj list to build the nodes array.
    for (var nodeId in scenario) {
        var node = this.getNodeWithId(nodeId);
        if (scenario[nodeId]['vertexValues']) {
            node.attrs = scenario[nodeId]['vertexValues'];
        }
        var adj = scenario[nodeId]['neighbors'];
        var msgs = scenario[nodeId]['outgoingMessages'];
        // Build this.messages
        if (msgs) {
            for(var receiverId in msgs) {
                this.messages.push({ sender: node,
                    receiver: this.getNodeWithId(receiverId),
                    message: msgs[receiverId]
                });
            }
        }
    }
}

/*
 * Adds new nodes and links to the graph without changing the existing structure.
 * @param {object} - scenario has the same format as above.
 * **NOTE** - This method will add news nodes and links without modifying
 * the existing structure. For instance, if the passed graph object does
 * not have a link, but it already exists in the graph, it will stay.
 */
Editor.prototype.addToGraph = function(scenario) {
    for (var nodeId in scenario) {
        // If this node is not present in the graph. Add it.
        this.addNode(nodeId);
        var neighbors = scenario[nodeId]['neighbors'];
        // For each neighbor, add the edge.
        for (var i = 0 ; i < neighbors.length; i++) {
            var neighborId = neighbors[i];
            // Add neighbor node if it doesn't exist.
            this.addNode(neighborId);
            // Addes edge, or ignores if already exists.
            this.addEdge(nodeId, neighborId);
        }
    }
}

/*
 * Shows the preloader and hides all other elements.
 */
Editor.prototype.showPreloader = function() {
    this.svg.selectAll('g').transition().style('opacity', 0);
    this.preloader.transition().style('opacity', 1);
}

/*
 * Hides the preloader and shows all other elements.
 */
Editor.prototype.hidePreloader = function() {
    this.svg.selectAll('g').transition().style('opacity', 1);
    this.preloader.transition().style('opacity', 0);
    this.restart();
}

/*
 * Enables the given node. Enabled nodes are shown as opaque.
 */
Editor.prototype.enableNode = function(nodeId) {
    this.getNodeWithId(nodeId).enabled = true;
}

/*
 * Disables the given node. 
 * Disabled nodes are shown as slightly transparent with outgoing messages removed.
 */
Editor.prototype.disableNode = function(nodeId) {
    this.getNodeWithId(nodeId).enabled = false;
    // Remove the outgoing Messages for this node.
    var toSplice = this.messages.filter(function(message) {
        return (message.sender.id === nodeId);
    });

    toSplice.map((function(message) {
        this.messages.splice(this.messages.indexOf(message), 1);
    }).bind(this));
}

/*
 * Colors the given node ids with the given color. Use this method to uncolor 
 * all the nodes (reset to default color) by calling colorNodes([], 'random', true);
 * @param {array} nodeIds - List of node ids.
 * @param {color} color - Color of these nodes.
 * @param {bool} [uncolorRest] - Optional parameter to reset the color of other nodes to default.
 */
Editor.prototype.colorNodes = function(nodeIds, color, uncolorRest) {
    // Set the color property of each node in this array. restart will reflect changes.
    for(var i = 0; i < nodeIds.length; i++) {
        var node = this.getNodeWithId(nodeIds[i]);
        if (node) {
            node.color = color;
        }
    }
    // If uncolorRest is specified
    if (uncolorRest) {
        for (var i = 0; i < this.nodes.length; i++) {
            // Not in nodeIds, uncolor it.
            if ($.inArray(this.nodes[i].id, nodeIds) === -1) {
                this.nodes[i].color = this.defaultColor;
            }
        }
    }
    this.restart();
}
