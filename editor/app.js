/*
 * Graph Editor is based on Directed Graph Editor by rkirsling http://bl.ocks.org/rkirsling/5001347
 */

/*
 * Editor is a class that encapsulates the graph editing window
 * @param {container, [undirected]} options - Initialize editor with these options. See @desc below.
 * @desc {options.container} - HTML element that contains the editor svg
 * @desc {options.undirected} - Indicate whether the graph is directed/undirected
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
            {id : '0', reflexive : false, attrs : []},
            {id : '1', reflexive : true, attrs : []},
            {id : '2', reflexive : false, attrs : []}
        ],
    this.lastNodeId = 2,
    this.links = [
            {source : this.nodes[0], target : this.nodes[1], left : false, right : true },
            {source : this.nodes[1], target : this.nodes[2], left : false, right : true }
        ];

    this.lastKeyDown = -1;
    this.init();
}

/*
 * Sets the size of the graph editing window.
 * The graph is always centered in the container according to these dimensions.
 */
Editor.prototype.setSize = function() {
    this.width = $(this.container).width();
    this.height = $(this.container).height();
}

/*
 * Resize the force layout. The D3 force layout controls the movement of the
 * svg elements within the container.
 */
Editor.prototype.resizeForce = function() {
    this.setSize();
    this.force.size([this.width, this.height]);
}

/*
 * Initializes the SVG element, along with marker and defs.
 */
Editor.prototype.initElements = function() {
    // Initialize colors for nodes
    this.colors = d3.scale.category10();

    // Creates the main SVG element and appends it to the container as the first child.
    // Set the SVG class to 'editor'.
    this.svg = d3.select(this.container)
                     .insert('svg', ':first-child')
                         .attr('class','editor')

   // Defines end arrow marker for graph links.
    this.svg.append('svg:defs')
                .append('svg:marker')
                    .attr('id', 'end-arrow')
                    .attr('viewBox', '0 -5 10 10')
                    .attr('refX', 6)
                    .attr('markerWidth', 3)
                    .attr('markerHeight', 3)
                    .attr('orient', 'auto')
                    .append('svg:path')
                        .attr('d', 'M0,-5L10,0L0,5')
                        .attr('fill', '#000');

    // Defines start arrow marker for graph links.
    this.svg.append('svg:defs')
                .append('svg:marker')
                    .attr('id', 'start-arrow')
                    .attr('viewBox', '0 -5 10 10')
                    .attr('refX', 4)
                    .attr('markerWidth', 3)
                    .attr('markerHeight', 3)
                    .attr('orient', 'auto')
                    .append('svg:path')
                        .attr('d', 'M10,-5L0,0L10,5')
                        .attr('fill', '#000')
}

/*
 * Binds the mouse and key events to the appropriate methods.
 */
Editor.prototype.initEvents = function() {
    // Mouse event vars - These variables are set (and reset) when the corresponding event occurs.
    this.selected_node = null;
    this.selected_link = null;
    this.mousedown_link = null;
    this.mousedown_node = null;
    this.mouseup_node = null;

    // Binds mouse down/up/move events on main SVG to appropriate methods. 
    // Used to create new nodes, create edges and dragging the graph.
    this.svg.on('mousedown', this.mousedown.bind(this))
            .on('mousemove', this.mousemove.bind(this))
            .on('mouseup', this.mouseup.bind(this));

    // Binds Key down/up events on the window to appropriate methods.
    d3.select(window)
          .on('keydown', this.keydown.bind(this))
          .on('keyup', this.keyup.bind(this));
}

/*
 * Initializes D3 force layout to update node/link location and orientation.
 */
Editor.prototype.initForce = function() {
    this.force = d3.layout.force()
                            .nodes(this.nodes)
                            .links(this.links)
                            .size([this.width, this.height])
                            .linkDistance(150)
                            .charge( -500 )
                            .on('tick', this.tick.bind(this))
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
 * Reset the mouse event variables to null.
 */
Editor.prototype.resetMouseVars = function() {
    this.mousedown_node = null;
    this.mouseup_node = null;
    this.mousedown_link = null;
}

/*
 * Called at a fixed time interval to update the nodes and edge positions.
 * Gives the fluid appearance to the editor.
 */
Editor.prototype.tick = function() {
    // draw directed edges with proper padding from node centers
    this.path.attr('d', function(d) {
        var sourcePadding = getPadding(d.source);
        var targetPadding = getPadding(d.target);

        var deltaX = d.target.x - d.source.x,
                deltaY = d.target.y - d.source.y,
                dist = Math.sqrt(deltaX * deltaX + deltaY * deltaY),
                normX = deltaX / dist,
                normY = deltaY / dist,
                sourcePadding = d.left ? sourcePadding[0] : sourcePadding[1],
                targetPadding = d.right ? targetPadding[0] : targetPadding[1],
                sourceX = d.source.x + (sourcePadding * normX),
                sourceY = d.source.y + (sourcePadding * normY),
                targetX = d.target.x - (targetPadding * normX),
                targetY = d.target.y - (targetPadding * normY);
        return 'M' + sourceX + ',' + sourceY + 'L' + targetX + ',' + targetY;
    });

    this.circle.attr('transform', function(d) {
        return 'translate(' + d.x + ',' + d.y + ')';
    });
}

/*
 * Returns the radius of the node.
 * Radius is not fixed since nodes with longer identifiers need a bigger circle.
 * @param {int} node - Node object whose radius is required.
 */
function getRadius(node) {
    return 14 + node.id.length * 3;
}

/*
 * Returns the padding of the node.
 * Padding is used by edges as an offset from the node center.
 * Padding is not fixed since nodes with longer identifiers need bigger circle.
 * @param {int} node - Node object whose padding is required.
 */
function getPadding(node) {
    return [17 + node.id.length * 3, 12 + node.id.length * 3];
}

/*
 * Updates existing links and adds new links
 */
Editor.prototype.restartLinks = function() {
    // path (link) group
    this.path = this.path.data(this.links);

    // update existing links
    this.path.classed('selected', (function(d) {
        return d === this.selected_link;
    }).bind(this))
            .style('marker-start', (function(d) {
                return d.left && !this.undirected ? 'url(#start-arrow)' : '';
            }).bind(this))
            .style('marker-end', (function(d) {
                return d.right && !this.undirected ? 'url(#end-arrow)' : '';
            }).bind(this));

    // add new links
    this.path.enter()
                .append('svg:path')
                    .attr('class', 'link')
                    .classed('selected', (function(d) {
                        return d === this.selected_link;
                    }).bind(this))
                    .style('marker-start', (function(d) {
                        if(d.left && !this.undirected) {
                            return  'url(#start-arrow)';
                        }
                        return '';
                    }).bind(this))
                    .style('marker-end', (function(d) {
                        if(d.right && !this.undirected) {
                            return 'url(#end-arrow)';
                        }
                        return '';
                    }).bind(this))
                    .on('mousedown', (function(d) {
                        if (d3.event.ctrlKey) {
                            return;
                        }

                        // select link
                        this.mousedown_link = d;
                        if (this.mousedown_link === this.selected_link) {
                            this.selected_link = null;
                        } else {
                            this.selected_link = this.mousedown_link;
                        }
                        this.selected_node = null;
                        this.restart();
                    }).bind(this));

    // remove old links
    this.path.exit().remove();
}

/*
 * Adds new nodes to the graph and binds mouse events.
 * Assumes that the data for this.circle is already set by the caller.
 * Creates 'circle' elements for each new node in this.nodes
 */
Editor.prototype.addNodes = function() {
    // Adds new nodes
    // The enter() call appends a 'g' element for each node in this.nodes
    // that is not present in this.circle already.
    var g = this.circle.enter().append('svg:g');

    // Draw the new node
    g.append('svg:circle')
        .attr('class', 'node')
        .attr('r', 14)
        .style('fill', (function(d) {
            return d === this.selected_node ?
                d3.rgb(this.colors(d.id)).brighter().toString() :
                this.colors(d.id);
        }).bind(this))
        .style('stroke', (function(d) {
            return d3.rgb(this.colors(d.id)).darker().toString();
        }).bind(this))
        .classed('reflexive', function(d) { return d.reflexive; })
        .on('mouseover', (function(d) {
            if (!this.mousedown_node || d === this.mousedown_node) {
                return;
            }
            // Enlarge target node
            d3.select(d3.event.target).attr('transform', 'scale(1.1)');
        }).bind(this))
        .on('mouseout', (function(d) {
            if (!this.mousedown_node || d === this.mousedown_node) {
                return;
            }
            // Unenlarge target node
            d3.select(d3.event.target).attr('transform', '');
        }).bind(this))
        .on('mousedown', (function(d) {
            if (d3.event.ctrlKey) {
                return;
            }
            // Select node
            this.mousedown_node = d;
            if (this.mousedown_node === this.selected_node) {
                this.selected_node = null;
            } else {
                this.selected_node = this.mousedown_node;
            }

            this.selected_link = null;

            // Reposition drag line
            this.drag_line
                .style('marker-end', 'url(#end-arrow)')
                .classed('hidden', false)
                .attr('d', 'M' + this.mousedown_node.x + ',' + this.mousedown_node.y + 'L' + this.mousedown_node.x + ',' + this.mousedown_node.y);
            this.restart();
        }).bind(this))
        .on('mouseup', (function(d) {
            if (!this.mousedown_node) {
                return;
            }

            this.drag_line
                .classed('hidden', true)
                .style('marker-end', '');

            // check for drag-to-self
            this.mouseup_node = d;
            if (this.mouseup_node === this.mousedown_node) {
                this.resetMouseVars();
                return;
            }

            // Unenlarge target node
            d3.select(d3.event.target).attr('transform', '');

            // Add link to graph (update if exists)
            // Note: Links are strictly source < target; arrows separately specified by booleans
            var source, target, direction;
            if (this.mousedown_node.id < this.mouseup_node.id) {
                source = this.mousedown_node;
                target = this.mouseup_node;
                direction = 'right';
            } else {
                source = this.mouseup_node;
                target = this.mousedown_node;
                direction = 'left';
            }

            var link;
            link = this.links.filter(function(l) {
                return (l.source === source && l.target === target);
            })[0];

            if (link) {
                link[direction] = true;
            } else {
                link = {source: source, target: target, left: false, right: false};
                link[direction] = true;
                this.links.push(link);
            }

            // Select new link
            this.selected_link = link;
            this.selected_node = null;
            this.restart();
        }).bind(this))
        .on('dblclick', (function(d) {
            if (this.dblnode) {
                this.dblnode({'event' : d3.event, 'node': d });
                this.restart();
            }
        }).bind(this));

    // Show node IDs
    g.append('svg:text')
        .attr('x', 0)
        .attr('y', 4)
        .attr('class', 'id')
        .text(function(d) { return d.id; });
}

/*
 * Updates existing nodes and adds new nodes.
 */
Editor.prototype.restartNodes = function() {
    // Set the circle group's data to this.nodes.
    // Note that nodes are identified by id, not their index in the array.
    this.circle = this.circle.data(this.nodes, function(d) { return d.id; });

    // Update existing nodes (reflexive & selected visual states)
    this.circle.selectAll('circle')
        .style('fill', (function(d) {
            return d === this.selected_node ?
                d3.rgb(this.colors(d.id)).brighter().toString() :
                this.colors(d.id);
        }).bind(this))
        .classed('reflexive', function(d) { return d.reflexive; })
        .attr('r', function(d) { return getRadius(d);  });

    this.addNodes();

    // Update node IDs
    this.circle.selectAll('text')
        .text(function(d) { return d.id; });

   // remove old nodes
    this.circle.exit().remove();
}

/*
 * Updates the graph. Called internally on various events.
 * May be called from the client after updating graph properties.
 */
Editor.prototype.restart = function() {
    this.resizeForce();
    this.restartLinks();
    this.restartNodes();

    // set the graph in motion
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

    // Insert new node at point
    var point = d3.mouse(d3.event.target),
        node = { id : ( ++this.lastNodeId).toString(), reflexive : false};
    node.x = point[0];
    node.y = point[1];
    node.attrs = []
    this.nodes.push(node);
    this.restart();
}

/*
 * Returns the index of the node with the given id in the nodes array.
 * @param {int} id - The identifier of the node.
 */
Editor.prototype.getNodeIndex = function(id) {
    return this.nodes.map(function(e) { return e.id }).indexOf(id);
}

/*
 * Returns the edge list.
 * Edge list is the representation of the graph as a list of edges.
 * An edge is represented as a vertex pair (u,v).
 */
Editor.prototype.getEdgeList = function() {
    edgeList = '';

    for (var i = 0; i < this.links.length; i++) {
        edgeList += this.links[i].source.id + '\t' + this.links[i].target.id + '\n';
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
    this.drag_line.attr('d', 'M' + this.mousedown_node.x + ',' + this.mousedown_node.y + 'L' + d3.mouse(this.svg[0][0])[0] + ',' + d3.mouse(this.svg[0][0])[1]);
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
 * Removes the links associated with a given node.
 * Used when a node is deleted.
 */
Editor.prototype.spliceLinksForNode = function(node) {
    var toSplice = this.links.filter(function(l) {
        return (l.source === node || l.target === node);
    });

    toSplice.map((function(l) {
        this.links.splice(this.links.indexOf(l), 1);
    }).bind(this));
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
