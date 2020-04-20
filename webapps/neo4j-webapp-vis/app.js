// Access the parameters that end-users filled in using webapp config
// For example, for a parameter called "input_dataset"
// input_dataset = dataiku.getWebAppConfig()['input_dataset']

/*
 * For more information, refer to the "Javascript API" documentation:
 * https://doc.dataiku.com/dss/latest/api/js/index.html
 */
// var newSelectNodeLabel = document.createElement('select');
// newSelectNodeLabel.className = "form-control";
// newSelectNodeLabel.id = "chooseNodeLabel";

// var newSelectRelationshipType = document.createElement('select');
// newSelectRelationshipType.className = "form-control";
// newSelectRelationshipType.id = "chooseRelationshipType";

var chooseNodeLabelElement = document.getElementById('chooseNodeLabel');
var chooseRelTypeElement = document.getElementById('chooseRelationshipType');

var chooseNodeCaptionElement = document.getElementById('chooseNodeCaption');
var chooseNodeSizeElement = document.getElementById('chooseNodeSize');
var chooseNodeColorElement = document.getElementById('chooseNodeColor');
var chooseRelCaptionElement = document.getElementById('chooseRelCaption');
var chooseRelSizeElement = document.getElementById('chooseRelSize');
// var chooseRelColorElement = document.getElementById('chooseRelColor');

var chooseQueryLimitElement = document.getElementById('queryLimit');
var drawButtonElement = document.getElementById('draw-button');

$(document).ready(function(){
        
    $.getJSON(getWebAppBackendUrl('/get_node_labels'), function(data){

         var selectNodeLabelHTML = "";

        var nodeLabels =  data["node_labels"];
        
        nodeLabels.forEach(myFunction); 
        function myFunction(item, index) { 
            selectNodeLabelHTML += "<option value='" + item + "'>" + item +"</option>";
        }
        
        chooseNodeLabelElement.innerHTML += selectNodeLabelHTML;
    });
    
    chooseNodeLabelElement.addEventListener("input", x => selectRelTypes(chooseNodeLabelElement.value, chooseRelTypeElement));
    
    chooseRelTypeElement.addEventListener("input", x => fillRelationshipFields(chooseRelTypeElement.value));
    
    chooseNodeLabelElement.addEventListener("input", x => fillNodeFields(chooseNodeLabelElement.value));

//     drawButtonElement.addEventListener('click', function (event) {

    
});     

function fillNodeFields(nodeLabel) { 
    //  Fill node caption   
    selectNodeProperties(nodeLabel, chooseNodeCaptionElement)
    //  Fill node size
    selectNodeProperties(nodeLabel, chooseNodeSizeElement, numerical=true)
    //  Fill node color
    selectNodeProperties(nodeLabel, chooseNodeColorElement, numerical=true)
    // TODO allow color for categorical properties or doubles
};

function fillRelationshipFields(relType) {
    //  Fill relationship caption   
    selectRelProperties(relType, chooseRelCaptionElement)
    //  Fill relationship size
    selectRelProperties(relType, chooseRelSizeElement, numerical=true)
    //  Fill relationship color
//     selectRelProperties(relType, chooseRelColorElement, numerical=true)
};

function selectRelTypes(nodeLabel, element) {

    var params = {"node_label": nodeLabel};
    
    $.getJSON(getWebAppBackendUrl('/get_relationship_types'), params, function(data){
        var selectRelationshipTypeHTML = "";
        var relTypes =  data["relationship_types"];

        relTypes.forEach(myFunction); 
        function myFunction(item, index) { 
            selectRelationshipTypeHTML += "<option value='" + item + "'>" + item +"</option>";
        }
        element.innerHTML = selectRelationshipTypeHTML;
        fillRelationshipFields(chooseRelTypeElement.value)
    });
};

function selectNodeProperties(nodeLabel, element, numerical=false) {
    var params = {"node_label": nodeLabel, "numerical": numerical};
    
    $.getJSON(getWebAppBackendUrl('/get_node_properties'), params, function(data){
        var selectNodePropertyHTML = "<option value='None'>None</option>";
        var nodeProperties =  data["node_properties"];

        nodeProperties.forEach(myFunction); 
        function myFunction(item, index) { 
            selectNodePropertyHTML += "<option value='" + item + "'>" + item +"</option>";
        }
        element.innerHTML = selectNodePropertyHTML;
    });
};

function selectRelProperties(relType, element, numerical=false) {
    var params = {"rel_type": relType, "numerical": numerical};
    
    $.getJSON(getWebAppBackendUrl('/get_rel_properties'), params, function(data){
        var selectRelPropertyHTML = "<option value='None'>None</option>";
        var relProperties =  data["rel_properties"];

        relProperties.forEach(myFunction); 
        function myFunction(item, index) { 
            selectRelPropertyHTML += "<option value='" + item + "'>" + item +"</option>";
        }
        element.innerHTML = selectRelPropertyHTML;
    });
};

document.getElementById('reset-button').addEventListener('click', function (event) {
    document.location.reload(true);
});

document.getElementById('draw-button').addEventListener('click', function (event) {
    console.warn("event: ", event)
    
//     let cypherQuery = document.getElementById('cypher-query');
//     let cypherQueryValue = cypherQuery.value || '(no query typed)';
    
    var nodeLabel = chooseNodeLabelElement.value;
    var relType = chooseRelTypeElement.value;
//     TODO test that nodeLabel and relType are not empty else display error message
    
    if (nodeLabel.length == 0 || relType.length == 0) {
        var err = "You must select a Node label and a Relationship type"
        const errElt = $('<div class="fatal-error" style="margin: 100px auto; text-align: center; color: var(--error-red)"></div>')
        errElt.text(err);
        $('#graph-container').html(errElt);
        return;
    }    

    var nodeCaption = chooseNodeCaptionElement.value;
    var nodeSize = chooseNodeSizeElement.value;
    var nodeColor = chooseNodeColorElement.value;
    
    var relCaption = chooseRelCaptionElement.value;
    var relSize = chooseRelSizeElement.value;
//     var relColor = chooseRelColorElement.value;
    
    var queryLimit = chooseQueryLimitElement.value;
//     TODO check that there is a value
        
    var container = document.getElementById('graph-container');
    
    var options = {
        nodes: {
            shape: 'dot',
            size: 30,
            scaling: {
                min: 20,
                max: 100,
                label: false
            }
        },
        edges: {
            scaling: {
                min: 1,
                max: 20,
                label: false
            }
        },
        physics: {
            forceAtlas2Based: {
                gravitationalConstant: -26,
                centralGravity: 0.005,
                springLength: 230,
                springConstant: 0.18
            },
            maxVelocity: 146,
            solver: 'forceAtlas2Based',
            timestep: 0.35,
            stabilization: {iterations: 150}
        }
    };
    
    var config = {
        "node_label": nodeLabel,
        "rel_type": relType,
        "node_caption": nodeCaption,
        "node_size": nodeSize,
        "node_color": nodeColor,
        "rel_caption": relCaption,
        "rel_size": relSize,
        "query_limit": queryLimit
//         "rel_color": relColor
    }
    console.warn("about to call backend with params: ", config)
    $.getJSON(getWebAppBackendUrl('/draw_graph'), config, function(results){
       
        console.warn("results: ", results)
        
        var data = {
            nodes: results["nodes"],
            edges: results["edges"],
        };
//         var options = results["options"];
        
        var network = new vis.Network(container, data, options);
        
        network.on("doubleClick", function (params) {
            
            var selectedNodeID = this.getSelectedNodes()[0];
            console.warn("doubleclick: ", params.pointer.DOM)
            
            if (selectedNodeID != null) {
                
                config["selected_node_id"] = selectedNodeID;
                
                console.log("selectedNodeID: ", selectedNodeID);
                $.getJSON(getWebAppBackendUrl('/draw_subgraph'), config, function(results){
       
                    console.warn("results: ", results)
                    data = {
                        nodes: results["nodes"],
                        edges: results["edges"],
                    };
                    network.setData(data);
                });
                
                
//                 var details = {
//                     selectedNodeID: params.nodes[0],
//                     config: config,
//                     container: container,
//                     options: options
//                 }
//                 var evt = new CustomEvent("selectNodeEvent", {detail: details});
//                 window.dispatchEvent(evt);
            }
        });
    });
});


//Listen for the event
window.addEventListener("selectNodeEvent", function(event) {
    console.warn('in selectNodeEvent:', event.detail);
    var config = event.detail.config;
    var container = event.detail.container;
    var options = event.detail.options;
//     console.warn("about to call backend with params: ", config)
    $.getJSON(getWebAppBackendUrl('/draw_graph'), config, function(results){
       
        console.warn("results: ", results)
        
        var data = {
            nodes: results["nodes"],
            edges: results["edges"],
        };
        
        var network = new vis.Network(container, data, options);
        
        network.on("doubleClick", function (params) {
//             var selectedNodeID = this.getNodeAt(params.pointer.DOM)
            console.warn("doubleclick: ", params.pointer.DOM)
            var selectedNodeID = this.getSelectedNodes();
            if (selectedNodeID != null) {
                console.log("selectedNodeID: ", selectedNodeID);
                var details = {
                    selectedNodeID: params.nodes[0],
                    config: config,
                    container: container,
                    options: options
                }
                var evt = new CustomEvent("selectNodeEvent", {detail: details});
                window.dispatchEvent(evt);
            }
        });
    });
}, false);





    
//     $.getJSON(getWebAppBackendUrl('/draw_graph'), 
//                             {'query': cypherQueryValue},
//         function(data) {
//             console.log('Received data from backend', data)
//             var container = document.getElementById('graph-container');
//             var network = new vis.Network(container, data["data"], data["options"]);
// //         const output = $('<pre />').text('Backend reply: ' + JSON.stringify(data));
// //         $('body').append(output)
//         });
    
    
//     // create an array with nodes
//     var nodes = [
//                 {id: 1, size: 50, label: 'Node 0', title: 'I have a popup!', group: 0},
//                 {id: 2, label: 'Node 2', title: 'I have a popup!', group: 1},
//                 {id: 3, label: 'Node 3', title: 'I have a popup!', group: 1},
//                 {id: 4, label: 'Node 4', title: 'I have a popup!', group: 1},
//                 {id: 5, label: 'Node 5', title: 'I have a popup!', group: 1}
//             ];
//     // create an array with edges
//     var edges = [
//                 {from: 1, to: 3},
//                 {from: 1, to: 2},
//                 {from: 2, to: 4},
//                 {from: 2, to: 5}
//             ];
//     // create a network
    
//     var data = {
//             nodes: nodes,
//             edges: edges
//         };
    
//     var network = new vis.Network(container, data, options);
    
      









// var newSelectDataset = document.createElement('select');
// newSelectDataset.className = "form-control";
// newSelectDataset.id = "chooseDataset";
    
// var newSelectColumnSource = document.createElement('select');
// newSelectColumnSource.className = "form-control";
// newSelectColumnSource.id = "chooseColumnSource";

// var newSelectColumnTarget = document.createElement('select');
// newSelectColumnTarget.className = "form-control";
// newSelectColumnTarget.id = "chooseColumnTarget";

// var newSelectColumnCut = document.createElement('select');
// newSelectColumnCut.className = "form-control";
// newSelectColumnCut.id = "chooseColumnCut";

// $(document).ready(function(){
    
//    document.getElementById('chooseDatasetDiv').appendChild(newSelectDataset);
//    document.getElementById('chooseColumnDivSource').appendChild(newSelectColumnSource);
//    document.getElementById('chooseColumnDivTarget').appendChild(newSelectColumnTarget);
//    document.getElementById('chooseColumnDivCut').appendChild(newSelectColumnCut);
   
//     $.getJSON(getWebAppBackendUrl('datasets'),function(data){
        
//         var selectDatasetHTML = "";
//         var dataset_names =  data.dataset_names,
//             datasetLen = dataset_names.length;
        
//         for(i = 0; i < datasetLen; i++){
//             selectDatasetHTML += "<option value='" + dataset_names[i] + "'>" + dataset_names[i]+"</option>";
//         };

//         newSelectDataset.innerHTML = "<option value='Dataset'>Dataset</option>" + selectDatasetHTML;
//         document.getElementById("chooseDataset").addEventListener("input", x => selectColumn(newSelectDataset.value));
      
        
 
//     });
        
// });






// let exportButton = document.getElementById('export-button');

// exportButton.addEventListener('click', function (event) {
//     let analysisName = document.getElementById('analysis-name');
//     let analysisValue = analysisName.value || '(no analysis name typed)';
//     let datasetName = document.getElementById('dataset-name');
//     let datasetValue = datasetName.value || '(no dataset chosen)';
//     let datasetColumn = document.getElementById('dataset-column');
//     let columnValue = datasetColumn.value || '(no column chosen)';
//     let rowsLimitValue = document.getElementById('rows-limit').checked;
//     let exportValue = document.querySelector('input[name="export"]:checked').value || '(no export format typed)';

//     alert('Now YOU should code something if you really want an export. For now, your parameters are: ' + analysisValue + ' / ' + datasetValue  + ' / ' + columnValue + ' / ' + rowsLimitValue + ' / ' + exportValue);
//     event.preventDefault();
// });

// /* Fetch dataset sample */
// let fetchButton = document.getElementById('fetch-button');
// let datasetName = document.getElementById('dataset-name');
// let messageContainer = document.getElementById('message');
// let selectedDataset = {};

// function displayMessage(messageText, messageClassname) {
//     messageContainer.innerHTML = messageText;
//     messageContainer.className = '';
//     if (messageClassname && messageClassname.length > 0) {
//         messageContainer.className = messageClassname;
//     }
// }

// function clearMessage() {
//     displayMessage('');
// }

// function displayFailure() {
//     displayMessage('The dataset cannot be retrieved. Please check the dataset name or the API Key\'s permissions in the "Settings" tab of the webapp.', 'error-message');
// }

// function displayDataFrame(dataFrame) {
//     let columnsNames = dataFrame.getColumnNames();
//     let line = '------------------------------';
//     let text = selectedDataset.name + '\n'
//         + line + '\n'
//         + dataFrame.getNbRows() + ' Rows\n'
//         + columnsNames.length + ' Columns\n'
//         + '\n' + line + '\n'
//         + 'Columns names: \n';
//     columnsNames.forEach(function(columnName) {
//         text += columnName + ', ';
//     });
//     displayMessage(text);
// }

// fetchButton.addEventListener('click', function(event) {
//     clearMessage();
//     selectedDataset.name = document.getElementById('dataset-to-fetch').value;
//     dataiku.fetch(selectedDataset.name, function(dataFrame) {
//         selectedDataset.dataFrame = dataFrame;
//         displayDataFrame(dataFrame);
//     }, function() {
//         displayFailure();
//     });
//     return false;
// });
// $.getJSON(getWebAppBackendUrl('/first_api_call'), function(data) {
//     console.log('Received data from backend', data)
//     const output = $('<pre />').text('Backend reply: ' + JSON.stringify(data));
//     $('body').append(output)
// });
