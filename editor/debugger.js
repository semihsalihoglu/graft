/*
 * Abstracts the debugger controls.
 */

/*
 * Debugger is a class that encapsulates the graph editor and debugging controls.
 * @param {editorContainer, nodeAttrContainer} options - Initialize debugger with these options.
 * @param options.editorContainer - Selector for the container of the graph editor.
 * @param options.nodeAttrContainer - Selector for the container of the node attr modal.
 * @param options.superstepControlsContainer - Selector for the container of the superstep controls.
 * @constructor
 */
function GiraphDebugger(options) {
    this.init(options);
    this.selectedNodeId = null; // Node that is currently double clicked;
    return this;
}

/*
 * Initializes the graph editor, node attr modal DOM elements.
 */
GiraphDebugger.prototype.init = function(options) {
    // Instantiate the editor object.
    this.editor = new Editor({
        'container' : options.editorContainer,
        'dblnode' : this.openNodeAttrs.bind(this)
    });

    this.initIds();
    // Must initialize these members as they are used by subsequent methods.
    this.nodeAttrContainer = options.nodeAttrContainer;
    this.superstepControlsContainer = options.superstepControlsContainer;
    this.initElements(options);
}

/*
 * Initialize DOM element Id constants
 */
GiraphDebugger.prototype.initIds = function() {
    this.ids = {
        // IDs of elements in node attribute modal.
        _nodeAttrModal : 'node-attr',
        _nodeAttrId : 'node-attr-id',
        _nodeAttrAttrs : 'node-attr-attrs',
        _nodeAttrGroupError : 'node-attr-group-error',
        _nodeAttrError : 'node-attr-error',
        _btnNodeAttrSave : 'btn-node-attr-save',
        _btnNodeAttrCancel : 'btn-node-attr-cancel',
        // IDs of elements in Superstep controls.
        _btnPrevStep : 'btn-prev-step',
        _btnNextStep : 'btn-next-step',
        _btnEditMode : 'btn-edit-mode',
        _btnFetchJob : 'btn-fetch-job'
    };
}

/*
 * Initializes the input elements inside the node attribute modal form.
 * @param nodeAttrForm - Form DOM object.
 */
GiraphDebugger.prototype.initInputElements = function(nodeAttrForm) {
   // Create form group for ID label and text box.
    var formGroup1 = $('<div />')
        .addClass('form-group')
        .appendTo(nodeAttrForm);

    // Create node ID label.
    var nodeAttrIdLabel = $('<label />')
        .attr('for', this.ids._nodeAttrId)
        .addClass('control-label col-sm-4')
        .html('Node ID:')
        .appendTo(formGroup1);

    // Create the ID input textbox.
    // Add it to a column div, which in turn is added to formgroup2.
    this.nodeAttrIdInput = $('<input>')
        .attr('type', 'text')
        .attr('id', this.ids._nodeAttrId)
        .addClass('form-control')
        .appendTo($('<div>').addClass('col-sm-8').appendTo(formGroup1));

    // Create the form group for attributes label and input.
    var formGroup2 = $('<div />')
        .addClass('form-group')
        .appendTo(nodeAttrForm);

    var nodeAttrAttributeLabel = $('<label />')
        .attr('for', this.ids._nodeAttrAttrs)
        .addClass('control-label col-sm-4')
        .html('Attributes: ')
        .appendTo(formGroup2);

    // Create the Attributes input textbox.
    // Add it to a column div, which in turn is added to formgroup2.
    this.nodeAttrAttrsInput = $('<input>')
        .attr('type', 'text')
        .attr('id', this._nodeAttrAttrs)
        .addClass('form-control')
        .appendTo($('<div>').addClass('col-sm-8').appendTo(formGroup2));

    // Create form group for buttons.
    var formGroupButtons = $('<div />')
        .addClass('form-group')
        .appendTo(nodeAttrForm);

    var buttonsContainer = $('<div />')
        .addClass('col-sm-12')
        .appendTo(formGroupButtons);

    this.btnNodeAttrSubmit = $('<button />')
        .attr('type', 'button')
        .addClass('btn btn-primary btn-sm editable-submit')
        .attr('id', this.ids._btnNodeAttrSave)
        .html('<i class="glyphicon glyphicon-ok"></i>')
        .appendTo(buttonsContainer);

    this.btnNodeAttrCancel = $('<button />')
        .attr('type', 'button')
        .addClass('btn btn-default btn-sm editable-cancel')
        .attr('id', this.ids._btnNodeAttrCancel)
        .html('<i class="glyphicon glyphicon-remove"></i>')
        .appendTo(buttonsContainer);

    this.nodeAttrGroupError = $('<div />')
        .addClass('form-group has-error')
        .attr('id', this.ids._nodeAttrGroupError)
        .hide()
        .appendTo(nodeAttrForm);

    var errorLabel = $('<label />')
        .addClass('control-label')
        .attr('id', this.ids._nodeAttrError)
        .html('Node ID must be unique')
        .appendTo($('<div class="col-sm-12"></div>').appendTo(this.nodeAttrGroupError));
}

/*
 * Initializes the message container and all elements within it.
 * Returns the message container DOM object.
 * @param nodeAttrForm - Form DOM object.
 */
GiraphDebugger.prototype.initMessageElements = function(nodeAttrForm) {
    var messageContainer = $('<div />')
        .appendTo(nodeAttrForm)

    var messageTabs = $('<ul />')
        .addClass('nav nav-tabs')
        .html('<li class="active"><a id="node-attr-sent" class="nav-msg" href="#!">Sent</a></li>' +
            '<li><a id="node-attr-received" class="nav-msg" href="#!">Received</a></li>')
        .appendTo(messageContainer);

    var tableContainer = $('<div />')
        .addClass('highlight')
        .appendTo(messageContainer);

    var messageTable = $('<table />')
        .addClass('table')
        .attr('id', 'node-attr-messages')
        .appendTo(messageContainer);
}

/*
 * Initializes Superstep controls. 
 * @param superstepControlsContainer - Selector for the superstep controls container.
 */
GiraphDebugger.prototype.initSuperstepControls = function(superstepControlsContainer) {
    // Create the form that fetches the superstep data from debugger server.
    this.formFetchJob = $('<div />')
        .attr('class', 'form-inline')
        .appendTo(superstepControlsContainer);

    var fetchJobGroup = $('<div />')
        .attr('class', 'form-group')
        .appendTo(this.formFetchJob);

    // Fetch job details for job id textbox. 
    var fetchJobIdInput = $('<input>')
        .attr('type', 'text')
        .attr('class', 'form-control ')
        .attr('placeholder', 'Job ID')
        .appendTo(fetchJobGroup);

    this.btnFetchJob = $('<button />')
        .attr('id', this.ids._btnFetchJob)
        .attr('type', 'button')
        .attr('class', 'btn btn-danger form-control')
        .html('Fetch')
        .appendTo(fetchJobGroup);
       
    // Initialize the actual controls.
    this.formControls = $("<div />")
        .attr('id', 'controls')
        .attr('class', 'form-inline')
        .hide()
        .appendTo(superstepControlsContainer);
    
    var controlsGroup = $("<div />")
        .attr('class', 'form-group')
        .appendTo(this.formControls);

    this.btnPrevStep = $("<button />")
        .attr('class', 'btn btn-default btn-step form-control')
        .attr('id', this.ids._btnPrevStep)
        .attr('disabled', 'true')
        .append(
            $('<span />')
            .attr('class', 'glyphicon glyphicon-chevron-left')
            .html(' Previous')
        )
        .appendTo(controlsGroup);

    var superstepLabel = $('<h2><span id="superstep">-1</span>' +
        '<small> Superstep</small></h2>')
        .appendTo(controlsGroup);

    this.btnNextStep = $('<button />')
        .attr('class', 'btn btn-default btn-step form-control')
        .attr('id', this.ids._btnNextStep)
        .append(
            $('<span />')
            .attr('class', 'glyphicon glyphicon-chevron-right')
            .html(' Next')
        )
        .appendTo(controlsGroup);

    // Return to the edit mode - Exiting the debug mode.
    this.btnEditMode = $('<button />')
        .attr('class', 'btn btn-default btn-step form-control')
        .attr('id', this.ids._btnEditMode)
        .append(
            $('<span />')
            .attr('class', 'glyphicon glyphicon-pencil')
            .html(' Edit Mode')
        )
        .appendTo(controlsGroup);

    // Initialize handlers for events
    this.initSuperstepControlEvents();
}

/*
 * Initializes the handlers of the elements on superstep controls.
 */
GiraphDebugger.prototype.initSuperstepControlEvents = function() {
    // On clicking Fetch button, send a request to the debugger server.
    $(this.btnFetchJob).click((function(event) {
        $(this.formFetchJob).hide(); 
    }).bind(this));
}

/*
 * Creates the document elements, like Node Attributes modal.
 */
GiraphDebugger.prototype.initElements = function() {
    // Div for the node attribute modal.
    this.nodeAttrModal = $('<div />')
        .attr('id', this.ids._nodeAttrModal)
        .hide()
        .appendTo(this.nodeAttrContainer);

    // Create a form and append to nodeAttr
    var nodeAttrForm = $('<div />')
        .addClass('form-horizontal')
        .appendTo(this.nodeAttrModal);

    this.initInputElements(nodeAttrForm);
    this.initMessageElements(nodeAttrForm);
    this.initSuperstepControls(this.superstepControlsContainer);

    // Attach events.
    // Click event of the Sent/Received tab buttons
    $('.nav-msg').click((function(event) {
        // Render the table
        var clickedId = event.target.id;
        var clickedSuffix = clickedId.substr(clickedId.lastIndexOf('-') + 1, clickedId.length);
        this.toggleMessageTabs(clickedSuffix);
        var messageData = clickedSuffix === 'sent' ?
            this.editor.getMessagesSentByNode(this.selectedNodeId) :
            this.editor.getMessagesReceivedByNode(this.selectedNodeId);
        this.showMessages(messageData);
    }).bind(this));
}

/*
 * This is a double-click handler.
 * Called from the editor when a node is double clicked.
 * Opens the node attribute modal with NodeId, Attributes and Messages.
 */
GiraphDebugger.prototype.openNodeAttrs = function(data) {
    // Set the currently double clicked node
    this.selectedNodeId = data.node.id;
    $(this.nodeAttrIdInput).attr('value', data.node.id)
        .attr('placeholder', data.node.id);

    $(this.nodeAttrAttrsInput).attr('value', data.node.attrs);
    $(this.nodeAttrGroupError).hide();

    $(this.nodeAttrModal).dialog({
        modal : true,
        width : 300,
        resizable : false,
        title : 'Node (ID: ' + data.node.id + ')',
        position : [data.event.clientX, data.event.clientY],
        closeOnEscape : true,
        hide : {effect : 'fade', duration : 100},
        close : (function() {
            this.selectedNodeId = null;
        }).bind(this)
    });

    $('.ui-widget-overlay').click(function() { $('#node-attr').dialog('close'); });
    $(this.btnNodeAttrCancel).click((function() {
        $(this.nodeAttrModal).dialog('close');
    }).bind(this));

    $(this.btnNodeAttrSubmit).unbind('click');
    $(this.btnNodeAttrSubmit).click((function() {
        var new_id = $(this.nodeAttrIdInput).val();
        var new_attrs_val = $(this.nodeAttrAttrsInput).val();
        var new_attrs = new_attrs_val.trim().length > 0 ? new_attrs_val.split(',') : [];

        if (data.editor.getNodeIndex(new_id) >= 0 && new_id != data.node.id) {
            $(this.nodeAttrGroupError).show();
            return;
        }

        var index = data.editor.getNodeIndex(data.node.id);
        data.editor.nodes[index].id = new_id;

        data.editor.nodes[index].attrs = new_attrs;
        data.editor.restart();
        $(this.nodeAttrModal).dialog('close');
    }).bind(this));

    // Set the 'Sent' tab as the active tab and show messages.
    this.toggleMessageTabs('sent');
    this.showMessages(data.editor.getMessagesSentByNode(this.selectedNodeId));
}

/*
 * Makes the clicked message tab active and the other inactive,
 * by setting/removing the 'active' classes on the corresponding elements.
 * @param - Suffix of the clicked element (one of 'sent'/'received')
 */
GiraphDebugger.prototype.toggleMessageTabs = function(activeSuffix) {
    // Remove the active class from the li element of the other link
    var removeSuffix = (activeSuffix === 'sent' ? 'received' : 'sent');
    $('#node-attr-' + removeSuffix).parent().removeClass('active');
    // Add the active class to the li element (parent) of the clicked link
    $('#node-attr-' + activeSuffix).parent().addClass('active');
}

/*
 * Populates the messages table on the node attr modal with the message data
 * @param messageData - The data of the sent/received messages from/to this node.
 */
GiraphDebugger.prototype.showMessages = function(messageData) {
    $('#node-attr-messages').html('');
    for (var nodeId in messageData) {
        var tr = document.createElement('tr');
        $(tr).html('<td>' + nodeId + '</td><td>' +
            messageData[nodeId] + '</td>');
        $('#node-attr-messages').append(tr);
    }
}
