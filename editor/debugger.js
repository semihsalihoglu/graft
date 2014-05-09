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

// TODO(vikesh) Move to a different js file.
function Utils() {}
/*
 * Returns the name of the trace file on the debugger server.
 */
Utils.getTraceFileName = function(jobId, superstepId, vertexId) {
    return "tr_" + jobId + "_stp_" + superstepId + "_vid_" + vertexId + ".tr";
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

    // Initialize current superstep to -2 (Not in debug mode)
    this.currentSuperstepNumber = -2;
    // ID of the job currently being debugged.
    this.currentJobId = null;
    // Minimum value of superstepNumber
    this.minSuperstepNumber = -1;
    // Maximum value of superstepNumber - Depends on the job.
    // TODO(vikesh): Fetch from debugger server in some AJAX call. Replace constant below.
    this.maxSuperstepNumber = 15;
    this.debuggerServerRoot = 'http://localhost:8000';

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
        _btnFetchJob : 'btn-fetch-job',
        _btnCaptureScenario : 'btn-capture-scenario'
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
    this.fetchJobIdInput = $('<input>')
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
    this.formControls = $('<div />')
        .attr('id', 'controls')
        .attr('class', 'form-inline')
        .hide()
        .appendTo(superstepControlsContainer);

    var controlsGroup = $('<div />')
        .attr('class', 'form-group')
        .appendTo(this.formControls);

    this.btnPrevStep = $('<button />')
        .attr('class', 'btn btn-default bt-step form-control')
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

    // Set this.superstepLabel to the actual label that will be updated.
    this.superstepLabel = $('#superstep');

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

    // Capture Scenario group
    var captureScenarioGroup = $('<div />')
        .attr('class', 'form-group')
        .appendTo(this.formControls);
    
    // Input text box to input the vertexId
    this.captureVertexIdInput = $('<input>')
        .attr('type', 'text')
        .attr('class', 'form-control ')
        .attr('placeholder', 'Vertex ID')
        .appendTo(captureScenarioGroup);

    // Capture Scenario button.
    this.btnCaptureScenario = $('<button>')
        .attr('type', 'button')
        .attr('id', this.ids._btnCaptureScenario)
        .attr('class', 'btn btn-primary form-control')
        .html('Capture Scenario')
        .appendTo(captureScenarioGroup);

    // Initialize handlers for events
    this.initSuperstepControlEvents();
}

/*
 * Initializes the handlers of the elements on superstep controls.
 */
GiraphDebugger.prototype.initSuperstepControlEvents = function() {
    // On clicking Fetch button, send a request to the debugger server
    // Fetch the scenario for this job for superstep -1
    $(this.btnFetchJob).click((function(event) {
        this.currentJobId = $(this.fetchJobIdInput).val();
        this.currentSuperstepNumber = 0;
        this.changeSuperstep(this.currentJobId, this.currentSuperstepNumber);
    }).bind(this));
    // On clicking the edit mode button, hide the superstep controls and show fetch form.
    $(this.btnEditMode).click((function(event) {
        this.editor.init();
        this.editor.restart();
        $(this.formControls).hide();
        $(this.formFetchJob).show();
    }).bind(this));

    // Handle the next and previous buttons on the superstep controls.
    $(this.btnNextStep).click((function(event) {
        this.currentSuperstepNumber += 1;
        this.changeSuperstep(this.currentJobId, this.currentSuperstepNumber);
    }).bind(this));

    $(this.btnPrevStep).click((function(event) {
        this.currentSuperstepNumber -= 1;
        this.changeSuperstep(this.currentJobId, this.currentSuperstepNumber);
    }).bind(this));

    // Handle the capture scenario button the superstep controls.
    $(this.btnCaptureScenario).click((function(event){
        var vertexId = $(this.captureVertexIdInput).val();
        var urlParams = $.param({
                'jobId' : this.currentJobId,
                'superstepId' : this.currentSuperstepNumber,
                'vertexId' : vertexId,
                'raw' : 'dummy'
        });
        //console.log(urlParams);
        location.href = this.debuggerServerRoot + "/scenario?" + urlParams;
    }).bind(this));
}

/*
 * Marshalls the scenario JSON for the editor. There are minor differences
 * in the schema of the editor and that returned by the debugger server. */
GiraphDebugger.prototype.marshallScenarioForEditor = function (data) {
    // Editor supports multiple attributes on each node, but
    // debugger server only returns a single vertexValue.
    var newData = $.extend({}, data);
    for (vertexId in data) {
        // Iterating over every vertex returned and creating a 
        // single element vertexValues array.
        newData[vertexId]['vertexValues'] = [data[vertexId]['vertexValue']];
    }
    console.log(newData);
    return newData;
}

/*
 * Fetches the data for this superstep, updates the superstep label, graph editor
 * and disables/enables the prev/next buttons.
 * @param {int} superstepNumber : Superstep to fetch the data for.
 */
GiraphDebugger.prototype.changeSuperstep = function(jobId, superstepNumber) {
    console.log("Changing Superstep to : " + superstepNumber);
    $(this.superstepLabel).html(superstepNumber);
    // Show preloader while AJAX request is in progress.
    this.editor.showPreloader();
    // Fetch from the debugger server.
    $.ajax({
        url : this.debuggerServerRoot + '/scenario',
        dataType : 'json',
        data: { 'jobId' : jobId, 'superstepId' : superstepNumber }
    })
    .done((function(data) {
        console.log(data);
        this.jobData = $.extend(this.jobData, data);
        this.editor.addToGraph(data);
        this.editor.updateGraphData(this.marshallScenarioForEditor(data));
        this.editor.hidePreloader();
        this.editor.restart();
        $(this.formFetchJob).hide();
        $(this.formControls).show();
    }).bind(this))
    .fail(function(error) {
        console.log(error);
    });
    this.editor.restart();
    $(this.btnNextStep).attr('disabled', superstepNumber === this.maxSuperstepNumber);
    $(this.btnPrevStep).attr('disabled', superstepNumber === this.minSuperstepNumber);
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
