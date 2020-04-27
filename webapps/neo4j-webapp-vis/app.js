
document.getElementById('reset-button').addEventListener('click', function (event) {
    document.location.reload(true);
});


// Global variables
var shapes = ['dot', 'ellipse', 'circle', 'database', 'box', 'text', 'star', 'triangle'];
var nodeLabels = getNodeLabels()
var relationTypes = getRelationTypes();

// --------- Relations graph-relations div ---------

var formRelationElement = document.getElementById("form-relation");
var relationRows = []
var i = 0;

document.getElementById('add-relation-button').addEventListener('click', function (event) {
    addRelation(i);
    i++;
});

document.getElementById('remove-relation-button').addEventListener('click', function (event) {
    if (i > 0) {
        i--;
        removeRelation(i);
    }
});

function addRelation(i) {
    var relationElement = document.createElement("div"); 
    relationElement.setAttribute('id', `relation-${i}`);
    formRelationElement.appendChild(relationElement);
        
    var array = [{label: "Source label", id: "selectSourceLabel", type: "select"},
                   {label: "Relationship type", id: "selectRelType", type: "select"},
                   {label: "Target label", id: "selectTargetLabel", type: "select"}]
    
    addRowElement(array, i, relationElement);
    
    relationRows.push([document.getElementById(array[0]['id'] + `-${i}`), 
                      document.getElementById(array[1]['id'] + `-${i}`),
                      document.getElementById(array[2]['id'] + `-${i}`)])
    
    populateSelectWithList(relationRows[i][0], nodeLabels);
    populateSelectWithList(relationRows[i][1], relationTypes);
    populateSelectWithList(relationRows[i][2], nodeLabels);
}

function removeRelation(i) {
    relationRows.pop()
    document.getElementById(`relation-${i}`).remove();
}

function getNodeLabels() {
    $.getJSON(getWebAppBackendUrl('/get_node_labels'), function(data){
        nodeLabels = data["node_labels"]; 
    });
};

function getRelationTypes() {
    $.getJSON(getWebAppBackendUrl('/get_relation_types'), function(data){
        relationTypes = data["relation_types"]; 
    });
};

function populateSelectWithList(element, list) {
    var selectHTML = "<option value=''></option>";
    list.forEach(myFunction); 
    function myFunction(item, index) { 
        selectHTML += `<option value='${item}'>${item}</option>`;
    }
    element.innerHTML = selectHTML;
};

function addRowElement (array, i, parentElement) { 

    array.forEach(function (item, index) {
        var div_multiple_fields = document.createElement("div"); 
        div_multiple_fields.setAttribute('class', 'multiple-fields-child');
        div_multiple_fields.setAttribute('style', `width: calc(100%/${array.length});`);
        parentElement.appendChild(div_multiple_fields);

        var div_field = document.createElement("div"); 
        div_field.setAttribute('class', 'field');
        div_multiple_fields.appendChild(div_field);

        div_field.innerHTML += `<label for='${item['id']}-${i}'>${item['label']}</label>`;

        var div_select = document.createElement("div"); 
        div_select.setAttribute('class', 'select');
        div_field.appendChild(div_select);
        
        var select = document.createElement("select"); 
        select.setAttribute('id', `${item['id']}-${i}`);
        div_select.appendChild(select);  

        var option = document.createElement("option"); 
//         option.setAttribute('value', 'test');
        select.appendChild(option); 
        
    });
}


// --------- Node graph-node-params div ---------

var formNodeParamsElement = document.getElementById("form-node-params");
var nodeParamsRows = [];
var n = 0;

document.getElementById('add-node-params-button').addEventListener('click', function (event) {
    addNodeParams(n);
    n++;
});

document.getElementById('remove-node-params-button').addEventListener('click', function (event) {
    if (n > 0) {
        n--;
        removeNodeParams(n);
    }
});

function addNodeParams(n) {
    var nodeParamsElement = document.createElement("div"); 
    nodeParamsElement.setAttribute('id', `node-params-${n}`);
    formNodeParamsElement.appendChild(nodeParamsElement);
        
    var array = [{label: "Label", id: "selectNodeLabel", type: "select"},
                {label: "Caption", id: "selectNodeCaption", type: "select"},
                {label: "Color", id: "selectNodeColor", type: "select"},
                {label: "Size", id: "selectNodeSize", type: "select"},
                {label: "Shape", id: "selectNodeShape", type: "select"}]
    
    addRowElement(array, n, nodeParamsElement);
    
    nodeParamsRows.push([document.getElementById(array[0]['id'] + `-${n}`),
                        document.getElementById(array[1]['id'] + `-${n}`),
                        document.getElementById(array[2]['id'] + `-${n}`),
                        document.getElementById(array[3]['id'] + `-${n}`),
                        document.getElementById(array[4]['id'] + `-${n}`)]);

    populateSelectWithList(nodeParamsRows[n][0], nodeLabels);
    populateSelectWithList(nodeParamsRows[n][4], shapes);
    
    nodeParamsRows[n][0].addEventListener("input", function () {
        selectNodeProperties(nodeParamsRows[n][0].value, nodeParamsRows[n][1]);
        selectNodeProperties(nodeParamsRows[n][0].value, nodeParamsRows[n][2]);
        selectNodeProperties(nodeParamsRows[n][0].value, nodeParamsRows[n][3], numerical=true);
    });
    
    
}

function removeNodeParams(n) {
    nodeParamsRows.pop()
    document.getElementById(`node-params-${n}`).remove();
}

function selectNodeProperties(nodeLabel, element, numerical=false) {
    var params = {node_label: nodeLabel, numerical: numerical};
    $.getJSON(getWebAppBackendUrl('/get_node_properties'), params, function(data){
        var selectNodePropertyHTML = "<option value='None'></option>";
        var nodeProperties =  data["node_properties"];
        nodeProperties.forEach(myFunction); 
        function myFunction(item, index) { 
            selectNodePropertyHTML += `<option value='${item}'>${item}</option>`;
        }
        element.innerHTML = selectNodePropertyHTML;
    });
};

// --------- Edge graph-relation-params div ---------

var formRelationParamsElement = document.getElementById("form-relation-params");
var relationParamsRows = [];
var r = 0;

document.getElementById('add-relation-params-button').addEventListener('click', function (event) {
    addRelationParams(r);
    r++;
});

document.getElementById('remove-relation-params-button').addEventListener('click', function (event) {
    if (r > 0) {
        r--;
        removeRelationParams(r);
    }
});

function addRelationParams(r) {
    var relationParamsElement = document.createElement("div"); 
    relationParamsElement.setAttribute('id', `relation-params-${r}`);
    formRelationParamsElement.appendChild(relationParamsElement);
        
    var array = [{label: "Type", id: "selectEdgeType", type: "select"},
                {label: "Caption", id: "selectEdgeCaption", type: "select"},
                {label: "Width", id: "selectEdgeWidth", type: "select"}]
    
    addRowElement(array, r, relationParamsElement);
    
    relationParamsRows.push([document.getElementById(array[0]['id'] + `-${r}`),
                        document.getElementById(array[1]['id'] + `-${r}`),
                        document.getElementById(array[2]['id'] + `-${r}`)]);

    populateSelectWithList(relationParamsRows[r][0], relationTypes);
    
    relationParamsRows[r][0].addEventListener("input", function () {
        selectRelProperties(relationParamsRows[r][0].value, relationParamsRows[r][1]);
        selectRelProperties(relationParamsRows[r][0].value, relationParamsRows[r][2]);
    });
}

function removeRelationParams(r) {
    relationParamsRows.pop();
    document.getElementById(`relation-params-${r}`).remove();
}

function selectRelProperties(relType, element, numerical=false) {
    var params = {"rel_type": relType, "numerical": numerical};
    $.getJSON(getWebAppBackendUrl('/get_rel_properties'), params, function(data){
        var selectRelPropertyHTML = "<option value='None'></option>";
        var relProperties =  data["rel_properties"];
        relProperties.forEach(myFunction); 
        function myFunction(item, index) { 
            selectRelPropertyHTML += `<option value='${item}'>${item}</option>`;
        }
        element.innerHTML = selectRelPropertyHTML;
    });
};


// ---------- Draw graph ----------

function getNodeLabelsSet(i) {
    var node_labels_set = new Set();
    for (var j = 0; j < i; j++) {
        node_labels_set.add(relationRows[j][0].value);
        node_labels_set.add(relationRows[j][2].value);
    }
    return node_labels_set;
}

function getRelationTypesSet(i) {
    var relation_types_set = new Set();
    for (var j = 0; j < i; j++) {
        relation_types_set.add(relationRows[j][1].value);
    }
    return relation_types_set;
}

function getNodeParams(n) {
    var node_params = {};
    var label, caption, size, color, shape;
    for (var j = 0; j < n; j++) {
        label = nodeParamsRows[j][0].value;
        caption = nodeParamsRows[j][1].value;
        color = nodeParamsRows[j][2].value;
        size = nodeParamsRows[j][3].value;
        shape = nodeParamsRows[j][4].value;
        node_params[label] = {caption: caption, size: size, color: color, shape: shape};
    }
    return node_params;
}

function getRelParams(r) {
    var rel_params = {};
    var type, caption, width;
    for (var j = 0; j < r; j++) {
        type = relationParamsRows[j][0].value;
        caption = relationParamsRows[j][1].value;
        width = relationParamsRows[j][2].value;
        rel_params[type] = {caption: caption, width: width};
    }
    return rel_params;
}


document.getElementById('draw-button').addEventListener('click', function (event) {
    document.getElementById('graph-container').innerHTML = "";
    
    console.log("var i: " + i + " var n: " + n + " var r: " + r)
    
    var node_labels_set = getNodeLabelsSet(i);
    var node_labels_array = Array.from(node_labels_set);
    var relation_types_set = getRelationTypesSet(i);
    
    console.log("node_labels_set is: ", node_labels_set);
    console.log("relation_types_set is: ", relation_types_set);
    
    var label_index = {}
    for (var j = 0; j < node_labels_array.length; j++) {
        label_index[node_labels_array[j]] = j;
    }
    
    var relations = [];
    for (var j = 0; j < i; j++) {
//         {src_id: "src_0", source: "Transfer", rel_id: "rel_0", relation: "OF_PLAYER", tgt_id: "tgt_0", target: "Player"},
        var rel = {};
        
        var source = relationRows[j][0].value;
        if (!(source)) {
            displayFatalError(`You must select a source node in relationship ${j+1} or remove it.`);
            return;
        }
        
        var relation = relationRows[j][1].value;
        if (!(relation)) {
            displayFatalError(`You must select a relationship type in relationship ${j+1} or remove it.`);
            return;
        }
        
        var target = relationRows[j][2].value;
        if (!(target)) {
            displayFatalError(`You must select a target node in relationship ${j+1} or remove it.`);
            return;
        }

        rel['source'] = source;
        rel['src_id'] = `n_${label_index[source]}`;
        rel['relation'] = relation;
        rel['rel_id'] = `r_${j}`;
        rel['target'] = target;
        if (source == target) {
            rel['tgt_id'] = `n_0${label_index[target]}`;
        } else {
            rel['tgt_id'] = `n_${label_index[target]}`;
        }
        relations.push(rel);
    }
    console.log("relations is: ", relations)
    
    var node_params = getNodeParams(n);
    console.log("node_params is: ", node_params);
    for (var key in node_params) {
        if (!(node_labels_set.has(key))) {
            console.log("key is: ", key)
            displayFatalError(`Node label '${key}' not selected in a relationship.`);
            return;
        }
    }

    var rel_params = getRelParams(r);
    console.log("rel_params is: ", rel_params);
    for (var key in rel_params) {
        if (!(relation_types_set.has(key))) {
            displayFatalError(`Relation type '${key}' not selected in a relationship.`);
            return;
        }
    }
    

        
        

    var query_limit = document.getElementById('queryLimit').value;
// //     TODO check that there is a value
        
    var container = document.getElementById('graph-container');
    
    var options = {
        nodes: {
            shape: 'dot',
            size: 30,
            scaling: {
                min: 30,
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
        physics: true
        // {
        //     forceAtlas2Based: {
        //         gravitationalConstant: -26,
        //         centralGravity: 0.005,
        //         springLength: 230,
        //         springConstant: 0.18
        //     },
        //     maxVelocity: 146,
        //     solver: 'forceAtlas2Based',
        //     timestep: 0.35,
        //     stabilization: {iterations: 150}
        // }
    };
    
//     var config = {
//         query_limit: 100,
//         relations: [
//                 {src_id: "n_0", source: "Transfer", rel_id: "r_0", relation: "OF_PLAYER", tgt_id: "n_1", target: "Player"},
//                 {src_id: "n_0", source: "Transfer", rel_id: "r_1", relation: "FROM_CLUB", tgt_id: "n_2", target: "Club"},
//                 {src_id: "n_0", source: "Transfer", rel_id: "r_2", relation: "TO_CLUB", tgt_id: "n_3", target: "Club"},
//                 {src_id: "n_1", source: "Player", rel_id: "r_3", relation: "PLAYS_FOR", tgt_id: "n_4", target: "Country"}
//         ],
//         node_params: {
//             "Transfer": {caption: "numericFee", size: "numericFee", color: "season", shape: "circle"},
//             "Player": {caption: "name", size: null, color: "position", shape: "dot"},
//             "Club": {caption: "name", size: null, color: null, shape: "database"},
//             "Country": {caption: "name", size: null, color: null, shape: "star"}
//         },
//         rel_params: {
//             "OF_PLAYER": {caption: "roles", width: null},
//             "FROM_CLUB": {caption: null, width: null},
//             "TO_CLUB": {caption: null, width: null},
//             "PLAYS_FOR": {caption: null, width: null}
//         }
//     };
    
    var config = {
        query_limit: query_limit,
        relations: relations,
        node_params: node_params,
        rel_params: rel_params
    };

    console.warn("about to call backend with config: ", config)
    $.getJSON(getWebAppBackendUrl('/draw_graph'), {config: JSON.stringify(config)}, function(results){
       
        console.warn("results: ", results)
        
        var data = {
            nodes: results["nodes"],
            edges: results["edges"],
        };
        
        var network = new vis.Network(container, data, options);
        
        network.on("doubleClick", function (params) {
            
            var selectedNodeID = this.getSelectedNodes()[0];
            console.warn("doubleclick: ", params.pointer.DOM)
            
            if (selectedNodeID != null) {
                
                config["selected_node_id"] = selectedNodeID;
                
                console.log("selectedNodeID: ", selectedNodeID);
                console.log("about to call draw_subgraph with: ", config)
                $.getJSON(getWebAppBackendUrl('/draw_subgraph'), {config: JSON.stringify(config)}, function(results){
       
                    console.warn("results: ", results)
                    data = {
                        nodes: results["nodes"],
                        edges: results["edges"],
                    };
                    network.setData(data);
                    network.focus(selectedNodeID);
                    console.warn("focus on: ", selectedNodeID)
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
    }).error(function(jqXHR, textStatus, errorThrown) {
        var errorElement = $('<div class="fatal-error" style="margin: 100px auto; text-align: center; color: var(--error-red)"></div>')
        var error = jqXHR.responseText;
        console.warn("error message: ", error);
        displayFatalError(error)
    })
});


function displayFatalError(error) {
    const errorElement = $('<div class="fatal-error" style="margin: 100px auto; text-align: center; color: var(--error-red)"></div>')
    errorElement.text(error);
    $('#graph-container').html(errorElement);
}

        