/*
 * ValidationPanel is a class that abstracts the message, vertex 
 * and exception details. It has three view modes - compact, preview and expanded.
 * @param {container, resizeCallback} options - Initialize panel with these options.
 * @param options.validationPanelContainer - Container of the panel.
 * @param {callback} options.resizeCallback - Called when manual resize of the panel is complete.
 * @param {object} options.editor - Reference to the graph editor object.
 */
function ValidationPanel(options) {
    // JSON object of the buttons appearing.
    // The key, i.e. M, E, V are used in the compact mode
    this.buttonData = {
        'M' : {
            fullName : 'Message Integrity',
            clickHandler : this.showMessageViolations.bind(this)
        },
        'E' : {
            fullName : 'Exceptions',
            clickHandler : this.showExceptions.bind(this)
        },
        'V' : {
            fullName : 'Vertex Integrity',
            clickHandler : this.showVertexViolations.bind(this)
        }
    }

    // Both in px
    this.compactWidth = 60;
    this.previewWidth = 170;
    // This is in %
    this.expandWidth = 55;
    this.state = ValidationPanel.StateEnum.COMPACT;
    this.container = options.container;
    this.resizeCallback = options.resizeCallback;
    this.debuggerServerRoot = options.debuggerServerRoot;
    this.editor = options.editor;
    // Which label is currently being shown
    this.currentLabel = null;
    
    $(this.container).css('height', this.height + 'px')
    // Make it resizable horizontally
    $(this.container).resizable({ handles : 'e', minWidth : this.previewWidth,
        stop: (function(event, ui) {
            this.resizeCallback();
        }).bind(this)
    });
    this.initElements();
    this.compact();
}

ValidationPanel.StateEnum = {
    COMPACT : 'compact',
    PREVIEW : 'preview',
    EXPAND : 'expand'
}

/*
 * Creates HTML elements for valpanel.
 */
ValidationPanel.prototype.initElements = function() {
    // Div to host the right arrow and close button on the top right.
    var iconsContainer = $('<div />')
                            .attr('class', 'valpanel-icons-container')
                            .appendTo(this.container)

    // Create a right-pointed arrow
    this.rightArrow = $('<span />')
        .attr('class', 'glyphicon glyphicon-circle-arrow-right')
        .appendTo(iconsContainer);

    // Create a close button - Clicking it will compact the panel.
    this.btnClose = $('<span />')
        .attr('class', 'glyphicon glyphicon-remove valpanel-btn-close')
        .click((function() { 
            this.compact(); 
        }).bind(this))
        .hide()
        .appendTo(iconsContainer);

    // Create all the buttons.
    this.btnContainer = $('<ul />')
        .attr('class', 'list-unstyled valpanel-btn-container')    
        .appendTo(this.container);

    // This is the container for the main content.
    this.contentContainer = $('<div />')
        .attr('class', 'valpanel-content-container')
        .hide()
        .appendTo(this.container);

    // Preloader reference.
    this.preloader = $('<div />')
        .attr('class', 'valpanel-preloader')
        .hide()
        .appendTo(this.container);

    for (var label in this.buttonData) {
        var button = $('<button />')
                        .attr('class', 'btn btn-success btn-valpanel')
                        .attr('id', this.btnLabelToId(label))
                        .attr('disabled', 'true')
                        .click(this.buttonData[label]['clickHandler']);
        var iconSpan = $('<span />')
            .appendTo(button);
        var textSpan = $("<span />")
            .html(' ' + label)
            .appendTo(button);
        // Associate this physical button element with the cache entry.
        this.buttonData[label].button = button;
        this.buttonData[label].iconSpan = iconSpan;
        this.buttonData[label].textSpan = textSpan;
        $(this.btnContainer).append(
            $("<li />").append(button)
        );
    }
}

ValidationPanel.prototype.btnLabelToId = function(label) {
    return 'btn-valpanel-' + label;
}

/*
 * Expands the width of the panel to show full names of each of the buttons.
 */
ValidationPanel.prototype.preview = function() {
    if (!$(this.container).is(':animated')) {
        // Set state to preview.
        this.btnContainer.removeClass(this.state);
        this.state = ValidationPanel.StateEnum.PREVIEW;
        this.btnContainer.addClass(this.state);
        $(this.btnClose).hide();
        $(this.contentContainer).hide();
        $(this.container).animate({ width: this.previewWidth + 'px'}, 300, 
            (function() { 
                this.resizeCallback(); 
        }).bind(this));

        // Expand names to full names 
        for (var label in this.buttonData) {
            var buttonData = this.buttonData[label];
            $(buttonData.textSpan).html(buttonData.fullName);
        }
    }
}

/*
 * Compacts the width of the panel to show only the labels of the buttons.
 */
ValidationPanel.prototype.compact = function() {
    if (!$(this.container).is(':animated')) {
        var prevState = this.state;
        this.currentLabel = null;
        this.state = ValidationPanel.StateEnum.COMPACT;
        // Uncolor all editor nodes
        this.editor.colorNodes([], null /* not required */, true);
        $(this.btnClose).hide();
        $(this.rightArrow).show();
        $(this.contentContainer).hide();
        $(this.container).animate({ width: this.compactWidth + 'px'}, 300,
            (function() {
                // Compact names to labels.
                for (var label in this.buttonData) {
                    var buttonData = this.buttonData[label];
                    $(buttonData.textSpan).html(label);
                }    
                this.btnContainer.removeClass(prevState);
                this.btnContainer.addClass(this.state);
                this.resizeCallback(); 
        }).bind(this));
    }
}

ValidationPanel.prototype.expand = function() {
    this.btnContainer.removeClass(this.state);
    this.state = ValidationPanel.StateEnum.EXPAND;
    this.btnContainer.addClass(this.state);
    // Show close button, hide right arrow, show content.
    $(this.btnClose).show();
    $(this.rightArrow).hide();
    $(this.container).animate({ width: this.expandWidth + '%'}, 500,
        (function() {
            $(this.contentContainer).show('slow');
            this.resizeCallback();
        }).bind(this));
}

/*
 * Fetch the message integrity violations from the debugger server
 * and construct a table to show the data.
 */
ValidationPanel.prototype.showMessageViolations = function() {
    this.expand();
    this.currentLabel = 'M';
    // Empty the content container and add violations table.
    // TODO(vikesh) Better visualization/search.
    this.contentContainer.empty();
    // The data should already be present in the buttonData cache.
    violations = this.buttonData[this.currentLabel].data.violations;
    var table = $("<table />")
        .attr('class', 'table')
        .html('<tr><th>Source</th><th>Destination</th><th>Message</th></tr>')
        .appendTo(this.contentContainer);

    if (violations) {
        for (var i = 0; i < violations.length; ++i) {
            var violation = violations[i];
            table.append($("<tr><td>{0}</td><td>{1}</td><td>{2}</td></tr>".format(violation.srcId, violation.destinationId, violation.message)));
        }
    }
}

/*
 * Fetch vertex value violations from the server and
 * construct a table to show the data.
 */
ValidationPanel.prototype.showVertexViolations = function() {
    this.expand();
    this.currentLabel = 'V';
    violations = this.buttonData[this.currentLabel].data.violations;
    // Empty the content container and add violations table.
    // TODO(vikesh) Better visualization/search.
    this.contentContainer.empty();
    var table = $("<table />")
        .attr('class', 'table')
        .html('<tr><th>Vertex ID</th><th>Vertex Value</th></tr>')
        .appendTo(this.contentContainer);
    var violationIds = [];
    if (violations) {
        for (var i = 0; violations && i < violations.length; ++i) {
            var violation = violations[i];
            violationIds.push(violation.vertexId);
            table.append($("<tr><td>{0}</td><td>{1}</td></tr>".format(violation.vertexId, violation.vertexValue)));
        }
    }
    // Color the vertices with violations
    this.editor.colorNodes(violationIds, this.editor.errorColor, true);
}

/*
 * Show exceptions for this superstep.
 */
ValidationPanel.prototype.showExceptions = function() {
    this.expand();
    this.currentLabel = 'E';
    // Empty the content container and add violations table.
    // TODO(vikesh) Better visualization/search.
    this.contentContainer.empty();
    var table = $("<table />")
        .attr('class', 'table')
        .html('<tr><th>Vertex ID</th><th>Message</th><th>Stack trace</th></tr>')
        .appendTo(this.contentContainer);
    var data = this.buttonData[this.currentLabel].data; 
    var violationIds = [];
    for (vertexId in data) {
        table.append($("<tr><td>{0}</td><td>{1}</td><td>{2}</td></tr>".format(vertexId, 
                    data[vertexId].exception.message, data[vertexId].exception.stackTrace)));
        violationIds.push(vertexId);
    }
    // Color the nodes with exception.
    this.editor.colorNodes(violationIds, this.editor.errorColor, true);
}



/*
 * Handle the received data from the debugger server.
 */
ValidationPanel.prototype.onReceiveData = function(buttonType) {
    return (function(response) {
        // Data is set here. The click handlers will simply use this data.
        this.buttonData[buttonType].data = response;
        this.buttonData[buttonType].button.attr('disabled', false);
        // No violations.
        if($.isEmptyObject(response)) {
            this.buttonData[buttonType].button.addClass('btn-success');
            this.buttonData[buttonType].button.removeClass('btn-danger');
        } else {
            this.buttonData[buttonType].button.addClass('btn-danger');
            this.buttonData[buttonType].button.removeClass('btn-success');
        }
        // If this is the currently selected label, update the contents.
        if (buttonType === this.currentLabel) {
            this.buttonData[buttonType].clickHandler(); 
        }
    }).bind(this);
}

/*
 * Sets the current jobId and superstepId. Expected to called by the 
 * orchestrator (debugger.js) while stepping through the job.
 * @param {jobId, superstepId} data
 * @param data.jobId - Current jobId
 * @param data.superstepId - Current superstepId
 */
ValidationPanel.prototype.setData = function(jobId, superstepId) {
    this.jobId = jobId;
    this.superstepId = superstepId;

    // setData makes AJAX calls to the debugger server for each button type
    for (var type in this.buttonData) {
        // Disable all buttons to begin with
        this.buttonData[type].button.attr('disabled', true);
        $.ajax({
                url: this.debuggerServerRoot + '/integrity',
                data: {'jobId' : this.jobId, 'superstepId' : this.superstepId, 'type' : type}
        })
        .retry({
                times: 3,
                timeout: 1000,
        })
        .done(this.onReceiveData(type))
        .fail((function(buttonLabel) {
                return function(response) {
                    noty({text : 'Failed to fetch data for ' + buttonLabel + 
                                '. Please check your network and debugger server.', type : 'error'});
                }
            })(this.buttonData[type].fullName))
        }
}

ValidationPanel.prototype.showPreloader = function() {
    this.preloader.show('slow');
}

ValidationPanel.prototype.hidePreloader = function() {
    this.preloader.hide('slow');
}
