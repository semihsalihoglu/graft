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

    if (options) {
        this.container = options['container'] ? options['container'] : this.container;
        this.undirected = options['undirected'] === true;
        if (options['dblnode']) {
            this.dblnode = options['dblnode'];
        }
    }

    this.setSize();

    this.nodes = [
            this.getNewNode('0'),
            this.getNewNode('1'),
            this.getNewNode('2')
        ],
    this.numNodes = 2,
    this.links = [
            {source : this.nodes[0], target : this.nodes[1], left : false, right : true },
            {source : this.nodes[1], target : this.nodes[2], left : false, right : true }
        ];

    // {sender: senderNodeObj, receiver: receiverNodeObj, message: message}
    this.messages  = [];

    this.lastKeyDown = -1;
    this.init();
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
    this.restartLinks();
    this.restartNodes();

    // Set the graph in motion
    this.force.start();
}

/*
 * Handles mousedown event.
 * Insert a new node if CTRL key is not pressed. Otherwise, drag the graph.
 */
Editor.prototype.mousedown = function() {
    this.svg.classed('active', true);

    if (d3.event.ctrlKey || this.mousedown_node || this.mousedown_link) {
        return;
    }

    // Insert new node at point.
    var point = d3.mouse(d3.event.target),
        node =  this.getNewNode((++this.numNodes).toString());
    node.x = point[0];
    node.y = point[1];
    this.nodes.push(node);
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
 *            adj: [adjId1, adjId2...],
 *            attrs: [attr1, attr2...],
 *            msgs: {
 *                    receiverId1: "message1",
 *                    receiverId2: "message2",
 *                    ...
 *                  }
 *          }
 * }
 */
Editor.prototype.buildGraphFromAdjList = function(adjList) {
    this.links = [];
    this.nodes = [];
    this.numNodes = 0;

    // Scan every node in adj list to build the nodes array.
    for (var nodeId in adjList) {
        var node = this.getNodeWithId(nodeId);
        if (node === null) {
            node = this.getNewNode(nodeId);
            this.numNodes++;
            this.nodes.push(node);
        }
        var adj = adjList[nodeId]['adj'];
        // For every node in the adj list of this node,
        // add the node to this.nodes and add the edge to this.links
        for (var i = 0; i < adj.length; i++) {
            var adjId = adj[i];
            var adjNode = this.getNodeWithId(adjId);
            if (adjNode === null) {
                adjNode = this.getNewNode(adjId);
                this.numNodes++;
                this.nodes.push(adjNode);
            }
            // Add the edge.
            // Note that edges are stored as source, target where source < target.
            if (nodeId < adjId) {
                this.links.push({source: node, target: adjNode, left: false, right: true});
            } else {
                this.links.push({source: adjNode, target: node, left: true, right: false});
            }
        }
    }
    this.updateGraphData(adjList);
    this.init();
    this.restart();
}

/*
 * Updates graph properties - node attributes and messages from adj list.
 * @param {object} graph - graph has the same format as adjList above,
 * but with 'adj' ignored. This method assumes the same graph structure,
 * only updates the node attributes and messages exchanged.
 */
Editor.prototype.updateGraphData = function(graph) {
    // Scan every node in adj list to build the nodes array.
    for (var nodeId in graph) {
        var node = this.getNodeWithId(nodeId);
    if (graph[nodeId]['attrs']) {
        node.attrs = graph[nodeId]['attrs'];
    }
        var adj = graph[nodeId]['adj'];
        var msgs = graph[nodeId]['msgs'];
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
}
