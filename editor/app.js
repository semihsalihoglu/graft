function Editor(options){
	this.container = "body";
	this.undirected = false;


	if(options){
		this.container = options["container"] ? options["container"] : this.container;
		this.undirected = options["undirected"]===true;

		if(options["dblnode"]){
			this.dblnode = options["dblnode"];
		}
	}

	this.setSize();

	this.nodes = [
			{id: "0", reflexive: false, attrs:[]},
			{id: "1", reflexive: true , attrs:[]},
			{id: "2", reflexive: false, attrs:[]}
		],
	this.lastNodeId = 2,
	this.links = [
			{source: this.nodes[0], target: this.nodes[1], left: false, right: true },
			{source: this.nodes[1], target: this.nodes[2], left: false, right: true }
		];

	this.lastKeyDown = -1;
	this.init();
}

Editor.prototype.setSize = function(){
	this.width=$(this.container).width();
	this.height=$(this.container).height();
}

Editor.prototype.resizeForce = function(){
	this.setSize();
	this.force.size([this.width, this.height]);
}
Editor.prototype.init = function(){
	this.colors = d3.scale.category10();
	this.svg = d3.select(this.container)
		.insert('svg', ':first-child')
		.attr('class','editor')

	// app starts here
	this.svg.on('mousedown', this.mousedown.bind(this))
		.on('mousemove', this.mousemove.bind(this))
		.on('mouseup', this.mouseup.bind(this));
	d3.select(window)
		.on('keydown', this.keydown.bind(this))
		.on('keyup', this.keyup.bind(this));

	// define arrow markers for graph links
	this.svg.append('svg:defs').append('svg:marker')
			.attr('id', 'end-arrow')
			.attr('viewBox', '0 -5 10 10')
			.attr('refX', 6)
			.attr('markerWidth', 3)
			.attr('markerHeight', 3)
			.attr('orient', 'auto')
		.append('svg:path')
			.attr('d', 'M0,-5L10,0L0,5')
			.attr('fill', '#000');

	this.svg.append('svg:defs').append('svg:marker')
			.attr('id', 'start-arrow')
			.attr('viewBox', '0 -5 10 10')
			.attr('refX', 4)
			.attr('markerWidth', 3)
			.attr('markerHeight', 3)
			.attr('orient', 'auto')
		.append('svg:path')
			.attr('d', 'M10,-5L0,0L10,5')
			.attr('fill', '#000')

	// line displayed when dragging new nodes
	this.drag_line = this.svg.append('svg:path')
		.attr('class', 'link dragline hidden')
		.attr('d', 'M0,0L0,0');

	// handles to link and node element groups
	this.path = this.svg.append('svg:g').selectAll('path'),
	this.circle = this.svg.append('svg:g').selectAll('g');

	// mouse event vars
	this.selected_node = null;
	this.selected_link = null;
	this.mousedown_link = null;
	this.mousedown_node = null;
	this.mouseup_node = null;

	this.force = d3.layout.force()
	    .nodes(this.nodes)
	    .links(this.links)
	    .size([this.width, this.height])
	    .linkDistance(150)
	    .charge(-500)
	    .on('tick', this.tick.bind(this))

	this.restart();
}

Editor.prototype.resetMouseVars = function() {
		this.mousedown_node = null;
		this.mouseup_node = null;
		this.mousedown_link = null;
}

Editor.prototype.tick = function(){
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

function getRadius(node)
{
	return 14+node.id.length*3;
}

function getPadding(node)
{
	return [17+node.id.length*3, 12+node.id.length*3];
}

// update graph (called when needed)
Editor.prototype.restart = function(){
	this.resizeForce();

  // path (link) group
  this.path = this.path.data(this.links);

  // update existing links
  this.path.classed('selected', (function(d) { return d === this.selected_link; }).bind(this))
    .style('marker-start', (function(d) { return d.left && !this.undirected ? 'url(#start-arrow)' : ''; }).bind(this))
    .style('marker-end', (function(d) { return d.right && !this.undirected ? 'url(#end-arrow)' : ''; }).bind(this));

  // add new links
  this.path.enter().append('svg:path')
    .attr('class', 'link')
    .classed('selected', (function(d) { return d === this.selected_link; }).bind(this))
    .style('marker-start', (function(d) { return d.left && !this.undirected ? 'url(#start-arrow)' : ''; }).bind(this))
    .style('marker-end', (function(d) { return d.right && !this.undirected ? 'url(#end-arrow)' : ''; }).bind(this))
    .on('mousedown', (function(d) {
      if(d3.event.ctrlKey) return;

      // select link
      this.mousedown_link = d;
      if(this.mousedown_link === this.selected_link) this.selected_link = null;
      else this.selected_link = this.mousedown_link;
      this.selected_node = null;
      this.restart();
    }).bind(this));

  // remove old links
  this.path.exit().remove();

  // circle (node) group
  // NB: the function arg is crucial here! nodes are known by id, not by index!
  this.circle = this.circle.data(this.nodes, function(d) { return d.id; });

  // update existing nodes (reflexive & selected visual states)
  this.circle.selectAll('circle')
    .style('fill', (function(d) { return (d === this.selected_node) ? d3.rgb(this.colors(d.id)).brighter().toString() : this.colors(d.id); }).bind(this))
    .classed('reflexive', function(d) { return d.reflexive; })
		.attr('r', function(d){ return getRadius(d);  });

	// Update node IDs
	this.circle.selectAll('text')
		.text(function(d){ return d.id; });

  // add new nodes
  var g = this.circle.enter().append('svg:g');

  g.append('svg:circle')
    .attr('class', 'node')
    .attr('r', 14)
    .style('fill', (function(d) { return (d === this.selected_node) ? d3.rgb(this.colors(d.id)).brighter().toString() : this.colors(d.id); }).bind(this))
    .style('stroke', (function(d) { return d3.rgb(this.colors(d.id)).darker().toString(); }).bind(this))
    .classed('reflexive', function(d) { return d.reflexive; })
    .on('mouseover', (function(d) {
      if(!this.mousedown_node || d === this.mousedown_node) return;
      // enlarge target node
      d3.select(d3.event.target).attr('transform', 'scale(1.1)');
    }).bind(this))
    .on('mouseout', (function(d) {
      if(!this.mousedown_node || d === this.mousedown_node) return;
      // unenlarge target node
      d3.select(d3.event.target).attr('transform', '');
    }).bind(this))
    .on('mousedown', (function(d) {
      if(d3.event.ctrlKey) return;

      // select node
      this.mousedown_node = d;
      if(this.mousedown_node === this.selected_node) this.selected_node = null;
      else this.selected_node = this.mousedown_node;
      this.selected_link = null;

      // reposition drag line
      this.drag_line
        .style('marker-end', 'url(#end-arrow)')
        .classed('hidden', false)
        .attr('d', 'M' + this.mousedown_node.x + ',' + this.mousedown_node.y + 'L' + this.mousedown_node.x + ',' + this.mousedown_node.y);
      this.restart();
    }).bind(this))
    .on('mouseup', (function(d) {
      if(!this.mousedown_node) return;

      // needed by FF
      this.drag_line
        .classed('hidden', true)
        .style('marker-end', '');

      // check for drag-to-self
      this.mouseup_node = d;
      if(this.mouseup_node === this.mousedown_node) { this.resetMouseVars(); return; }

      // unenlarge target node
      d3.select(d3.event.target).attr('transform', '');

      // add link to graph (update if exists)
      // NB: links are strictly source < target; arrows separately specified by booleans
      var source, target, direction;
      if(this.mousedown_node.id < this.mouseup_node.id) {
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

      if(link) {
        link[direction] = true;
      } else {
        link = {source: source, target: target, left: false, right: false};
        link[direction] = true;
        this.links.push(link);
      }

      // select new link
      this.selected_link = link;
     	this.selected_node = null;
      this.restart();
    }).bind(this))
		.on('dblclick', (function(d) {
			if(this.dblnode){
				this.dblnode({"event":d3.event, "node":d});
				this.restart();
			}
		}).bind(this));

  // show node IDs
  g.append('svg:text')
      .attr('x', 0)
      .attr('y', 4)
      .attr('class', 'id')
      .text(function(d) { return d.id; });

  // remove old nodes
  this.circle.exit().remove();

  // set the graph in motion
  this.force.start();
}

Editor.prototype.mousedown = function() {
  // prevent I-bar on drag
  //d3.event.preventDefault();
  
  // because :active only works in WebKit?
  this.svg.classed('active', true);

  if(d3.event.ctrlKey || this.mousedown_node || this.mousedown_link) return;

  // insert new node at point
  var point = d3.mouse(d3.event.target),
      node = {id: (++this.lastNodeId).toString(), reflexive: false};
  node.x = point[0];
  node.y = point[1];
	node.attrs = []
  this.nodes.push(node);
  this.restart();
}

Editor.prototype.getNodeIndex = function(id){
	return this.nodes.map(function(e){ return e.id }).indexOf(id);
}

Editor.prototype.getEdgeList = function(){
	edgeList = "";

	for(var i=0; i<this.links.length; i++){
			edgeList += this.links[i].source.id + "\t" + this.links[i].target.id + "\n";
	}

	return edgeList;
}

Editor.prototype.getAdjList = function(){
	adjList = "";

	for(var i=0; i<this.nodes.length; i++){
		var id = this.nodes[i].id;
		var nodes = [];

		for(var j=0;j<this.links.length; j++){
			var link = this.links[j];

			if((link.left===true || this.undirected===true) && link.target.id===id) {
				nodes.push(link.source.id);
			}
			if((link.right===true || this.undirected===true) && link.source.id===id) {
				nodes.push(link.target.id);
			}
		}

		if(nodes.length>0){
			adjList += id + "\t";

			for(var j=0;j<nodes.length;j++)
			{
				adjList += nodes[j] + "\t";
			}
			adjList += i!=this.nodes.length-1 ? "\n" : "";
		}
	}
		
	return adjList;	
}

Editor.prototype.getNodeList  = function(){
	nodeList = "";

	for(var i=0; i<this.nodes.length; i++){
		nodeList += this.nodes[i].id + "\t" + this.nodes[i].attrs.join(",");
		nodeList += (i!=this.nodes.length-1 ? "\n" : "");
	}

	return nodeList;
}

Editor.prototype.mousemove = function() {
  if(!this.mousedown_node) return;

  // update drag line
  this.drag_line.attr('d', 'M' + this.mousedown_node.x + ',' + this.mousedown_node.y + 'L' + d3.mouse(this.svg[0][0])[0] + ',' + d3.mouse(this.svg[0][0])[1]);
  this.restart();
}

Editor.prototype.mouseup = function() {
  if(this.mousedown_node) {
    // hide drag line
    this.drag_line
      .classed('hidden', true)
      .style('marker-end', '');
  }

  // because :active only works in WebKit?
  this.svg.classed('active', false);

  // clear mouse event vars
  this.resetMouseVars();
}

Editor.prototype.spliceLinksForNode = function(node) {
  var toSplice = this.links.filter(function(l) {
    return (l.source === node || l.target === node);
  });
  toSplice.map(function(l) {
    this.links.splice(this.links.indexOf(l), 1);
  });
}

// only respond once per keydown

Editor.prototype.keydown = function() {
  // d3.event.preventDefault();

  if(this.lastKeyDown !== -1) return;
  this.lastKeyDown = d3.event.keyCode;

  // ctrl
  if(d3.event.keyCode === 17) {
    this.circle.call(this.force.drag);
    this.svg.classed('ctrl', true);
  }

  if(!this.selected_node && !this.selected_link) return;
  switch(d3.event.keyCode) {
    case 8: // backspace
    case 46: // delete
      if(this.selected_node) {
        this.nodes.splice(nodes.indexOf(this.selected_node), 1);
        this.spliceLinksForNode(this.selected_node);
      } else if(this.selected_link) {
        this.links.splice(this.links.indexOf(this.selected_link), 1);
      }
      this.selected_link = null;
      this.selected_node = null;
      this.restart();
      break;
    case 66: // B
      if(this.selected_link) {
        // set link direction to both left and right
        this.selected_link.left = true;
        this.selected_link.right = true;
      }
      this.restart();
      break;
    case 76: // L
      if(this.selected_link) {
        // set link direction to left only
        this.selected_link.left = true;
        this.selected_link.right = false;
      }
      this.restart();
      break;
    case 82: // R
      if(this.selected_node) {
        // toggle node reflexivity
        this.selected_node.reflexive = !this.selected_node.reflexive;
      } else if(this.selected_link) {
        // set link direction to right only
        this.selected_link.left = false;
        this.selected_link.right = true;
      }
      this.restart();
      break;
  }
}

Editor.prototype.keyup = function() {
  this.lastKeyDown = -1;

  // ctrl
  if(d3.event.keyCode === 17) {
    this.circle
      .on('mousedown.drag', null)
      .on('touchstart.drag', null);
    this.svg.classed('ctrl', false);
  }
}
