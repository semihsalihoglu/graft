/*
 * ValidationPanel is a class that abstracts the message, vertex 
 * and exception details. It has three view modes - compact, preview and expanded.
 * @param {container, resizeCallback} options - Initialize panel with these options.
 * @param options.validationPanelContainer - Container of the panel.
 * @param {callback} options.resizeCallback - Called when manual resize of the panel is complete.
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
    this.container = options.container;
    this.resizeCallback = options.resizeCallback;
    
    $(this.container).css('height', this.height + 'px')
    // Make it resizable horizontally
    $(this.container).resizable({ handles : 'e', minWidth : this.compactWidth,
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

    for (var label in this.buttonData) {
        var button = $('<button />')
                        .attr('class', 'btn btn-success btn-valpanel')
                        .attr('id', this.btnLabelToId(label))
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
        this.state = ValidationPanel.StateEnum.COMPACT;
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
        }).bind(this));
}

ValidationPanel.prototype.dummy = function() {
    this.expand();
    this.contentContainer.empty();
    this.contentContainer.html("Hello WOrld");
}
