/*
 * ValidationPanel is a class that abstracts the message, vertex 
 * and exception details. It has three view modes - compact, preview and expanded.
 * @param {container, height} options - Initialize panel with these options.
 * @param options.validationPanelContainer - Container of the panel.
 * @param {int} options.height - Height of the panel. Use integral value without px suffix.
 */
function ValidationPanel(options) {
    // JSON object of the buttons appearing.
    // The key, i.e. M, E, V are used in the compact mode
    this.buttonData = {
        'M' : {
            fullName : 'Message Integrity',
            clickHandler : this.dummy.bind(this)
        },
        'E' : {
            fullName : 'Exceptions',
            clickHandler : this.dummy
        },
        'V' : {
            fullName : 'Vertex Integrity',
            clickHandler : this.dummy
        }
    }

    // Both in px
    this.compactWidth = 60;
    this.previewWidth = 170;
    // This is in %
    this.expandWidth = 95;
    this.state = ValidationPanel.StateEnum.COMPACT;
    this.height = options.height;
    this.container = options.container;
    
    $(this.container).css('height', this.height + 'px')
        .css('width', this.compactWidth + 'px');
    this.initElements();
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
    // Create a right-pointed arrow
    var rightArrow = $('<span />')
        .attr('class', 'glyphicon glyphicon-circle-arrow-right valpanel-arrow-right')
        .appendTo(
            $("<div />")
                .attr('class', 'valpanel-arrow-container')
                .appendTo(this.container)
        );

    // Create all the buttons.
    var buttonList = $("<ul />")
        .attr('class', 'list-unstyled valpanel-btn-container')    
        .appendTo(this.container);

    for (var label in this.buttonData) {
        var button = $("<button />")
                        .attr('class', 'btn btn-success btn-valpanel')
                        .attr('id', this.btnLabelToId(label))
                        .data('label', label)
                        .html(label)
                        .click(this.buttonData[label]['clickHandler']);
        // Associate this physical button element with the cache entry.
        this.buttonData[label].button = button;
        $(buttonList).append(
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
    // Set state to preview.
    this.state = ValidationPanel.StateEnum.PREVIEW;
    $(this.container).animate({ width: this.previewWidth + 'px'}, 300);
    // Expand names to full names 
    for (var label in this.buttonData) {
        var buttonData = this.buttonData[label];
        $(buttonData['button']).html(buttonData['fullName']);
    }
}

/*
 * Compacts the width of the panel to show only the labels of the buttons.
 */
ValidationPanel.prototype.compact = function() {
    this.state = ValidationPanel.StateEnum.COMPACT;
    $(this.container).animate({ width: this.compactWidth + 'px'}, 300,
        (function() {
        // Expand names to full names 
        for (var label in this.buttonData) {
            var buttonData = this.buttonData[label];
            $(buttonData['button']).html(label);
        }    
    }).bind(this));
}

ValidationPanel.prototype.expand = function() {
    this.state = ValidationPanel.StateEnum.EXPAND;
    $(this.container).animate({ width: this.expandWidth + '%'}, 500,
        function() {
            console.log('hell');
    });
}

ValidationPanel.prototype.dummy = function() {
    this.expand();
}
