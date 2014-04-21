/*
 * Abstracts the debugger controls. 
 */

/*
 * Debugger is a class that encapsulates the graph editor and debugging controls.
 * @param {editor, nodeAttrs} options - Initialize debugger with these options.
 * @param {container, [undirected]} options.editor - Editor options.
 * @param { } options.nodeAttrs
 * @constructor
 */

function GiraphDebugger(options) {
    this.init(options.editor);
    this.selectedNodeId = null; // Node that is currently double clicked;
    return this;
}

GiraphDebugger.prototype.init = function(editorOptions) {
    // Instantiate the editor object.
    this.editor = new Editor({
        'container' : editorOptions.container,
        'dblnode' : this.openNodeAttrs.bind(this)
    });

    this.nodeAttrContainer = '#node-attr-container';
    this.initElements();
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

    // Create Node ID Label
    var nodeAttrIdLabel = $('<label />')
        .attr('for', 'node-attr-id')
        .addClass('control-label col-sm-4')
        .html('Node ID:')
        .appendTo(formGroup1);

    // Create the ID input textbox
    // Add it to a column div, which in turn is added to formgroup2
    var nodeAttrIdInput = $('<input>')
        .attr('type', 'text')
        .attr('id', 'node-attr-id')
        .addClass('form-control')
        .appendTo($('<div>').addClass('col-sm-8').appendTo(formGroup1));

    // Create the form group for attributes label and input
    var formGroup2 = $('<div />')
        .addClass('form-group')
        .appendTo(nodeAttrForm);

    var nodeAttrAttributeLabel = $('<label />')
        .attr('for', 'node-attr-attrs')
        .addClass('control-label col-sm-4')
        .html('Attributes: ')
        .appendTo(formGroup2);

    // Create the Attributes input textbox
    // Add it to a column div, which in turn is added to formgroup2
    var nodeAttrAttributeInput = $('<input>')
        .attr('type', 'text')
        .attr('id', 'node-attr-attrs')
        .addClass('form-control')
        .appendTo($('<div>').addClass('col-sm-8').appendTo(formGroup2));

    // Create form group for buttons
    var formGroupButtons = $('<div />')
        .addClass('form-group')
        .appendTo(nodeAttrForm);

    var buttonsContainer = $('<div />')
        .addClass('col-sm-12')
        .appendTo(formGroupButtons);

    var btnSubmit = $('<button />')
        .attr('type', 'button')
        .addClass('btn btn-primary btn-sm editable-submit')
        .attr('id', 'btn-node-attr-save')
        .html('<i class="glyphicon glyphicon-ok"></i>')
        .appendTo(buttonsContainer);

    var btnCancel = $('<button />')
        .attr('type', 'button')
        .addClass('btn btn-default btn-sm editable-cancel')
        .attr('id', 'btn-node-attr-cancel')
        .html('<i class="glyphicon glyphicon-remove"></i>')
        .appendTo(buttonsContainer);

    var errorContainer = $('<div />')
        .addClass('form-group has-error')
        .attr('id', 'node-attr-group-error')
        .hide()
        .appendTo(nodeAttrForm);

    var errorLabel = $('<label />')
        .addClass('control-label')
        .attr('id', 'node-attr-error')
        .html('Node ID must be unique')
        .appendTo($('<div class="col-sm-12"></div>').appendTo(errorContainer));
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
 * Creates the document elements, like Node Attributes modal.
 */
GiraphDebugger.prototype.initElements = function() {

    // Div for the node attribute modal.
    var nodeAttr = $('<div></div>')
        .attr('id', 'node-attr')
        .hide()
        .appendTo(this.nodeAttrContainer);

    // Create a form and append to nodeAttr
    var nodeAttrForm = $('<div></div>')
        .addClass('form-horizontal')
        .appendTo(nodeAttr);

    this.initInputElements(nodeAttrForm);
    this.initMessageElements(nodeAttrForm);

    // Attach events.
    
    // Click event of the Sent/Received tab buttons
    $(".nav-msg").click((function(event) { 
        // Render the table
        var clickedId = event.target.id;
        var clickedSuffix = clickedId.substr(clickedId.lastIndexOf('-')+1, clickedId.length);
        this.toggleMessageTabs(clickedSuffix);
        var messageData = clickedSuffix ==="sent" ?
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
    $('#node-attr-id').attr('value', data.node.id);
    $('#node-attr-id').attr('placeholder', data.node.id);
    $('#node-attr-attrs').attr('value', data.node.attrs);
    $('#node-attr-group-error').hide();

    $('#node-attr').dialog({
            modal : true,
            width : 300,
            resizable :false,
            title :'Node (ID: ' + data.node.id + ')' ,
            position : [data.event.clientX, data.event.clientY],
            closeOnEscape : true,
            hide : {effect : 'fade', duration:100},
            close: (function(){
                this.selectedNodeId = null;
            }).bind(this)
        });

        $('.ui-widget-overlay').click(function() { $('#node-attr').dialog('close'); });
        $('#btn-node-attr-cancel').click(function() { 
                $('#node-attr').dialog('close');
        });

        $('#btn-node-attr-save').unbind('click');
        $('#btn-node-attr-save').click(function() { 
                var new_id = $('#node-attr-id').val();
                var new_attrs_val = $('#node-attr-attrs').val();
                var new_attrs = new_attrs_val.trim().length > 0 ? new_attrs_val.split(',') : [];

                if (data.editor.getNodeIndex(new_id) >= 0 && new_id != data.node.id) {
                    $('#node-attr-group-error').show();
                    return;
                }

                var index = data.editor.getNodeIndex(data.node.id);
                data.editor.nodes[index].id = new_id;

                data.editor.nodes[index].attrs = new_attrs;
                data.editor.restart();
                $('#node-attr').dialog('close');
    });

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
    var removeSuffix = (activeSuffix ==="sent" ? "received" : "sent");
    $("#node-attr-"+removeSuffix).parent().removeClass("active");
    // Add the active class to the li element (parent) of the clicked link
    $("#node-attr-"+activeSuffix).parent().addClass("active");
}

/* 
 * Populates the messages table on the node attr modal with the message data
 * @param messageData - The data of the sent/received messages from/to this node.
 */
GiraphDebugger.prototype.showMessages = function(messageData) {
    $("#node-attr-messages").html("");
        for(var nodeId in messageData) {
            var tr = document.createElement("tr");
            $(tr).html("<td>" + nodeId + "</td><td>" + 
                    messageData[nodeId] + "</td>");
            $("#node-attr-messages").append(tr);
    }
}
