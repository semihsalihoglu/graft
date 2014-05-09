//TODO(vikesh): Move to Utils
Array.prototype.remove = function(from, to) {
  var rest = this.slice((to || from) + 1 || this.length);
  this.length = from < 0 ? this.length + from : from;
  return this.push.apply(this, rest);
};

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
                     .html('')
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
                        .attr('fill', '#000');
    // Append the preloader
    // Dimensions of the image are 128x128
    var preloaderX = this.width / 2 - 64;
    var preloaderY = this.height / 2 - 64;
    this.preloader = this.svg.append('svg:g')
                                 .attr('transform', 'translate(' + preloaderX + ',' + preloaderY + ')')
                                 .attr('opacity', 0);

    this.preloader.append('svg:image')
                      .attr('xlink:href', 'img/preloader.gif')
                      .attr('width', '128')
                      .attr('height', '128');
    this.preloader.append('svg:text')
                      .text('Loading')
                      .attr('x', '40')
                      .attr('y', '128');
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
    // Radius is detemined by multiplyiing the max of length of node ID
    // and node value (first attribute) by a factor and adding a constant.
    // If node value is not present, only node id length is used.
    return 16 + Math.max(node.id.length, node.attrs.length > 0 ? node.attrs[0].toString().length : 0) * 3;
}

/*
 * Returns the padding of the node.
 * Padding is used by edges as an offset from the node center.
 * Padding is not fixed since nodes with longer identifiers need bigger circle.
 * @param {int} node - Node object whose padding is required.
 */
function getPadding(node) {
    // Offset is detemined by multiplyiing the max of length of node ID
    // and node value (first attribute) by a factor and adding a constant.
    // If node value is not present, only node id length is used.
    var nodeOffset = Math.max(node.id.length, node.attrs.length > 0 ? node.attrs[0].toString().length : 0) * 3;
    return [19 + nodeOffset, 12  + nodeOffset];
}

/*
 * Returns a new node object.
 * @param {string} id - Identifier of the node.
 */
Editor.prototype.getNewNode = function(id) {
    return {id : id, reflexive : false, attrs : [], x: 0, y: 0, enabled: true};
}

/*
 * Returns a new link (edge) object from the node IDs of the logical edge.
 * @param {string} sourceNodeId - The ID of the source node in the logical edge.
 * @param {string} targetNodeId - The ID of the target node in the logical edge.
 * @desc - Logical edge means, "Edge from node with ID x to node with ID y".
 * It implicitly captures the direction. However, the link objects have
 * the 'left' and 'right' properties to denote direction. Also, source strictly < target.
 * Therefore, the source and target may not match that of the logical edge, but the
 * direction will compensate for the mismatch.
 */
Editor.prototype.getNewLink = function(sourceNodeId, targetNodeId) {
    var source, target, direction;
    if (sourceNodeId < targetNodeId) {
        source = sourceNodeId;
        target = targetNodeId;
        direction = 'right';
    } else {
        source = targetNodeId;
        target = sourceNodeId;
        direction = 'left';
    }
    link = {source: this.getNodeWithId(source), target: this.getNodeWithId(target), left: false, right: false};
    link[direction] = true;
    return link;
}

/*
 * Adds a new link object to the links array or updates an existing link.
 * @param {string} sourceNodeId - Id of the source node in the logical edge.
 * @param {string} targetNodeid - Id of the target node in the logical edge.
 */
Editor.prototype.addEdge = function(sourceNodeId, targetNodeId) {
    console.log('Adding edge: ' + sourceNodeId + ' -> ' + targetNodeId);
    // Get the new link object.
    var newLink = this.getNewLink(sourceNodeId, targetNodeId);
    // Check if a link with these source and target Ids already exists.
    var existingLink = this.links.filter(function(l) {
        return (l.source === newLink.source && l.target === newLink.target);
    })[0];

    // Add link to graph (update if exists).
    if (existingLink) {
        // Set the existingLink directions to true if either
        // newLink or existingLink denote the edge.
        existingLink.left |= newLink.left;
        existingLink.right |= newLink.right;
        return existingLink;
    } else {
        this.links.push(newLink);
        return newLink;
    }
}

/*
 * Adds node with nodeId to the graph (or ignores if already exists).
 * Returns the added (or already existing) node.
 * @param [{string}] nodeId - ID of the node to add. If not provided, adds
 * a new node with a new nodeId.
 * TODO(vikesh): Incremental nodeIds are buggy. May cause conflict. Use unique identifiers.
 */
Editor.prototype.addNode = function(nodeId) {
    if (!nodeId) {
        nodeId = (this.numNodes + 1).toString();
    }
    var newNode = this.getNodeWithId(nodeId);
    if (!newNode) {
        newNode = this.getNewNode(nodeId);
        this.nodes.push(newNode);
        this.numNodes++;
    }
    return newNode;
}

/*
 * Updates existing links and adds new links.
 */
Editor.prototype.restartLinks = function() {
    // path (link) group
    this.path = this.path.data(this.links);

    // Update existing links
    this.path.classed('selected', (function(d) {
        return d === this.selected_link;
    }).bind(this))
             .style('marker-start', (function(d) {
                 return d.left && !this.undirected ? 'url(#start-arrow)' : '';
             }).bind(this))
             .style('marker-end', (function(d) {
                 return d.right && !this.undirected ? 'url(#end-arrow)' : '';
             }).bind(this));

    // Add new links.
    // For each link in the bound data but not in elements group, enter()
    // selection calls everything that follows once.
    // Note that links are stored as source, target where source < target.
    // If the link is from source -> target, it's a 'right' link.
    // If the link is from target -> source, it's a 'left' link.
    // A right link has end marker at the target side.
    // A left link has a start marker at the source side.
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

                         // Select link
                         this.mousedown_link = d;
                         if (this.mousedown_link === this.selected_link) {
                             this.selected_link = null;
                         } else {
                             this.selected_link = this.mousedown_link;
                         }
                         this.selected_node = null;
                         this.restart();
                     }).bind(this));

    // Remove old links.
    this.path.exit().remove();
}

/*
 * Adds new nodes to the graph and binds mouse events.
 * Assumes that the data for this.circle is already set by the caller.
 * Creates 'circle' elements for each new node in this.nodes
 */
Editor.prototype.addNodes = function() {
    // Adds new nodes.
    // The enter() call appends a 'g' element for each node in this.nodes.
    // that is not present in this.circle already.
    var g = this.circle.enter().append('svg:g');

    // Draw the new node.
    g.attr('class', 'node-container')
         .append('svg:circle')
         .attr('class', 'node')
         .attr('r', (function(d) {
             return getRadius(d);
         }).bind(this))
         .style('fill', '#FFFDDB')
         .style('stroke', '#000000')
         .classed('reflexive', function(d) { return d.reflexive; })
         .on('mouseover', (function(d) {
             if (!this.mousedown_node || d === this.mousedown_node) {
                 return;
             }
             // Enlarge target node.
             d3.select(d3.event.target).attr('transform', 'scale(1.1)');
         }).bind(this))
         .on('mouseout', (function(d) {
             if (!this.mousedown_node || d === this.mousedown_node) {
                 return;
             }
             // Unenlarge target node.
             d3.select(d3.event.target).attr('transform', '');
         }).bind(this))
         .on('mousedown', (function(d) {
             if (d3.event.ctrlKey) {
                 return;
             }
             // Select node.
             this.mousedown_node = d;
             if (this.mousedown_node === this.selected_node) {
                 this.selected_node = null;
             } else {
                 this.selected_node = this.mousedown_node;
             }

             this.selected_link = null;

             // Reposition drag line.
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

             // Check for drag-to-self.
             this.mouseup_node = d;
             if (this.mouseup_node === this.mousedown_node) {
                 this.resetMouseVars();
                 return;
             }

             // Unenlarge target node to default size.
             d3.select(d3.event.target).attr('transform', '');

             // Add link to graph (update if exists).
             var newLink = this.addEdge(this.mousedown_node.id, this.mouseup_node.id);
             this.selected_link = newLink;
             this.restart();
         }).bind(this))
         .on('dblclick', (function(d) {
             if (this.dblnode) {
                 this.dblnode({'event' : d3.event, 'node': d, editor : this });
                 this.restart();
             }
         }).bind(this));

    // Show node IDs
    g.append('svg:text')
        .attr('x', 0)
        .attr('y', 4)
        .attr('class', 'tid')
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
        .style('fill', '#FFFDDB')
        .classed('reflexive', function(d) { return d.reflexive; })
        .attr('r', function(d) { return getRadius(d);  });

    // If node is not enabled, set its opacity to 0.2    
    this.circle.style('opacity', function(d) { return d.enabled ? 1 : 0.2; });
    this.addNodes();

    // Update node IDs
    var el = this.circle.selectAll('text').text('');
    el.append('tspan')
          .text(function(d) {
              return d.id;
          })
          .attr('x', 0)
          .attr('dy', function(d) {
              return d.attrs.length > 0 ? '-8' : '0 ';
          })
          .attr('class', 'id');

    // Node value (if present) is added/updated here
    el.append('tspan')
          .text(function(d) {
              return d.attrs[0];
          })
          .attr('x', 0)
          .attr('dy', function(d) {
              return d.attrs.length > 0 ? '18' : '0';
          })
          .attr('class', 'vval');

   // remove old nodes
    this.circle.exit().remove();
}

/*
 * Returns the index of the node with the given id in the nodes array.
 * @param {string} id - The identifier of the node.
 */
Editor.prototype.getNodeIndex = function(id) {
    return this.nodes.map(function(e) { return e.id }).indexOf(id);
}

/*
 * Returns the node object with the given id, null if node is not present.
 * @param {string} id - The identifier of the node.
 */
Editor.prototype.getNodeWithId = function(id) {
    var index = this.getNodeIndex(id);
    return index >= 0 ? this.nodes[index] : null;
}

/*
 * Returns true if the node with the given ID is present in the graph.
 * @param {string} id - The identifier of the node.
 */
Editor.prototype.containsNode = function(id) {
    return this.getNodeIndex(id) >= 0;
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
