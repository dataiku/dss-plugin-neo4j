var app = angular.module("graphApp", []);

app.controller("GraphController", function($scope, $http) {
    $scope.template = "globalView"
    // initialize first row of select
    $scope.error = {}
    $scope.relationships = []
    $scope.nodeProperties = []
    $scope.nodeParams = {}
    
    $scope.uniqueRelations = new Set()
    $scope.selectedRelationship = {
        source: "",
        relation: "",
        target: ""
    }
    $scope.queryLimit = 20
    $scope.nodeLabels = [] 
    $scope.usedNodeIndex = {}
    
    // params for local view
    $scope.localSelection = {}

    $scope.manipulation = {}
    $scope.test = {}


    console.warn("$scope.selectedRelationship: ", $scope.selectedRelationship)

    $scope.setTemplate = function(template) {
        if ($scope.template != template) {
            $scope.template = template
        }
    }

    function initUsedNodeIndex(nodeLabels) {
        // initialise the usedNodeIndex dictionary with 0 values for each node label
        var results = {}
        for (var i = 0; i < nodeLabels.length; i++) {
            results[nodeLabels[i]] = 0
        }
        return results
    }
    
    function createNodeIndexes(nodeLabels, usedNodeIndex) {
        // create a list of node label with some index as suffix based on usedNodeIndex
        var results = []
        for (var i = 0; i < nodeLabels.length; i++) {
            for (var j = 0; j <= usedNodeIndex[nodeLabels[i]]; j++) {
                results.push(`${nodeLabels[i]} - ${j+1}`)
            }
        }
        return results
    }
    
    function updateUsedNodeIndex(relationships, usedNodeIndex) {
        // update usedNodeIndex based on all already selected relationships (used when adding or removing a relationship)
        // first reset all indexes to 0 (because of when removing a relationship ...)
        for (var label in usedNodeIndex) {
            usedNodeIndex[label] = 0
        }
        for (var i = 0; i < relationships.length; i++) {
            var src = relationships[i].source.split(" - ")[0]
            var src_idx = relationships[i].source.split(" - ")[1]
            if (usedNodeIndex[src] < src_idx) {
                usedNodeIndex[src] = src_idx
            }
            var tgt = relationships[i].target.split(" - ")[0]
            var tgt_idx = relationships[i].target.split(" - ")[1]
            if (usedNodeIndex[tgt] < tgt_idx) {
                usedNodeIndex[tgt] = tgt_idx
            }
        }
    }
    
    function createTemporaryNodeIndexes(nodeLabels, usedNodeIndex, selectedLabel) {
        // when a user selects a :selectedLabel node, it adds to the other select an extra index for the same label
        var results = []
        for (var i = 0; i < nodeLabels.length; i++) {
            for (var j = 0; j <= usedNodeIndex[nodeLabels[i]]; j++) {
                results.push(`${nodeLabels[i]} - ${j+1}`)
            }
            if (nodeLabels[i] == selectedLabel) {
                results.push(`${nodeLabels[i]} - ${j+1}`)
            }
        }
        return results
    }
    
    $http.get(getWebAppBackendUrl("/get_node_labels"))
    .then(function(response) {
        $scope.nodeLabels = response.data.node_labels
        $scope.usedNodeIndex = initUsedNodeIndex($scope.nodeLabels)
        var nodeLabelsIndex = createNodeIndexes($scope.nodeLabels, $scope.usedNodeIndex)
        $scope.sourceNodeLabels = nodeLabelsIndex
        $scope.targetNodeLabels = nodeLabelsIndex
    }, function(e) {
        $scope.error.msg = e.data;
    });
    
    $http.get(getWebAppBackendUrl("/get_relation_types"))
    .then(function(response) {
        $scope.relationTypes = response.data.relation_types
    }, function(e) {
        $scope.error.msg = e.data;
    });
    
    $scope.nodeChange = function (currentNode) {
        // when the user selects a node in a relation, it updates the node label indexes of the other node (source or target)
       console.log("currentNode: ", currentNode)
       if (currentNode == 'Source') {
           var src = $scope.selectedRelationship.source.split(" - ")[0]
           var src_idx = parseInt($scope.selectedRelationship.source.split(" - ")[1])
           if (src_idx > $scope.usedNodeIndex[src]) {
               $scope.targetNodeLabels = createTemporaryNodeIndexes($scope.nodeLabels, $scope.usedNodeIndex, src)
           }
       } else {
           var tgt = $scope.selectedRelationship.target.split(" - ")[0]
           var tgt_idx = parseInt($scope.selectedRelationship.target.split(" - ")[1])
           if (tgt_idx > $scope.usedNodeIndex[tgt]) {
               $scope.sourceNodeLabels = createTemporaryNodeIndexes($scope.nodeLabels, $scope.usedNodeIndex, tgt)
           }
       }
    }
    
    // add a new relation to the table (when complete)
    $scope.addRelation = function () {
        console.log("nodeParams: ", $scope.nodeParams)
        var relationship = {
            source: $scope.selectedRelationship.source,
            relation: $scope.selectedRelationship.relation,
            target: $scope.selectedRelationship.target
        };
        console.log("relationship: ", relationship)
        if (relationship.source == "" || relationship.relation == "" || relationship.target == "") {
            console.warn("Relationship not complete, cannot add a new one !")
        } else {
            $scope.relationships.push(relationship);
            updateUsedNodeIndex($scope.relationships, $scope.usedNodeIndex)
            
            var nodeLabelsIndex = createNodeIndexes($scope.nodeLabels, $scope.usedNodeIndex)
            $scope.sourceNodeLabels = nodeLabelsIndex
            $scope.targetNodeLabels = nodeLabelsIndex
            
            var source_label = relationship.source.split(" - ")[0]
            if (!($scope.nodeProperties.map(a=>a.label).includes(source_label))) {
                $http.post(getWebAppBackendUrl("/get_node_properties"), {node_label: source_label})
                .then(function(response) {
                    $scope.nodeProperties.push(
                        {
                            label: source_label,
                            all: response.data.node_properties_all,
                            num: response.data.node_properties_num
                        }
                    )
                }, function(e) {
                    $scope.error.msg = e.data;
                });
            }
            var target_label = relationship.target.split(" - ")[0]
            if (!($scope.nodeProperties.map(a=>a.label).includes(target_label))) {
                $http.post(getWebAppBackendUrl("/get_node_properties"), {node_label: target_label})
                .then(function(response) {
                    if (!($scope.nodeProperties.map(a=>a.label).includes(target_label))) {
                        $scope.nodeProperties.push(
                            {
                                label: target_label,
                                all: response.data.node_properties_all,
                                num: response.data.node_properties_num
                            }
                        )
                    }
                }, function(e) {
                    $scope.error.msg = e.data;
                });
            }
            $scope.selectedRelationship.source = ""
            $scope.selectedRelationship.relation = ""
            $scope.selectedRelationship.target = ""
        }
        console.log("nodeProperties: ", $scope.nodeProperties)
    };

    // remove a relation (a row) from the table (only the last completed one)
    $scope.removeRelation = function (index) {
        // TODO remove from nodeProperties nodes of the last relationship (the one to be removed) if there are not used in previous relationships
        var source_label = $scope.relationships[index].source.split(" - ")[0]
        var target_label = $scope.relationships[index].target.split(" - ")[0]
        
        $scope.relationships.splice(index, 1)
        updateUsedNodeIndex($scope.relationships, $scope.usedNodeIndex)
        
        var nodeLabelsIndex = createNodeIndexes($scope.nodeLabels, $scope.usedNodeIndex)
        $scope.sourceNodeLabels = nodeLabelsIndex
        $scope.targetNodeLabels = nodeLabelsIndex
        
        if ($scope.usedNodeIndex[source_label] == 0) {
            $scope.nodeProperties = $scope.nodeProperties.filter(item => item.label != source_label);
        }
        if ($scope.usedNodeIndex[target_label] == 0) {
            $scope.nodeProperties = $scope.nodeProperties.filter(item => item.label != target_label);
        }
        console.log("$scope.nodeProperties: ", $scope.nodeProperties)
    }
    
    $scope.reset = function() {
       document.location.reload(true);
    }

    $scope.addProperty = function (mode) {
        if (mode == 'editNode') {
            $scope.manipulation.editNodeProperties.push({key: "", value: ""})
        } else if (mode == 'addNode') {
            $scope.manipulation.addNodeProperties.push({key: "", value: ""})
        } else {
            $scope.manipulation.edgeProperties.push({key: "", value: ""})
        } 
        // else if (mode == 'addEdge') {
        //     $scope.manipulation.addEdgeProperties.push({key: "", value: ""})
        // }
    }
    $scope.addNode = function(data, cancelAction, callback) {
        // $scope.manipulation.addNodeLabel
        $scope.manipulation.addNodeProperties = [{key: "", value: ""}]

        document.getElementById("add-node-saveButton").onclick = $scope.saveNodeData.bind(
            this,
            data,
            'addNode',
            callback
        );
        document.getElementById("add-node-cancelButton").onclick = cancelAction.bind(
            this,
            'addNode'
        );
        document.getElementById("add-node-popUp").style.display = "block";
        // force angularjs to refresh to bind ng-model 
        $scope.$apply()
    }

    $scope.editNode = function(data, cancelAction, callback) {
        // document.getElementById("node-label").value = data.label;
        $scope.manipulation.editNodePopup = data.title.split("</b>")[0].split(">")[1]
        console.log("data.label: ", data.label)

        $http.post(getWebAppBackendUrl("/get_selected_node_properties"), {node_id: data.id})
        .then(function(response) {
            $scope.manipulation.editNodeProperties = response.data.properties;
        }, function(e) {
            $scope.error.msg = e.data;
        });
        
        document.getElementById("edit-node-saveButton").onclick = $scope.saveNodeData.bind(
          this,
          data,
          'editNode',
          callback
        );
        document.getElementById("edit-node-cancelButton").onclick = cancelAction.bind(
          this,
          callback
        );
        document.getElementById("edit-node-popUp").style.display = "block";
        // force angularjs to refresh to bind ng-model 
        $scope.$apply()
    }

    $scope.clearNodePopUp = function(mode) {
        if (mode == 'editNode') {
            document.getElementById("edit-node-saveButton").onclick = null;
            document.getElementById("edit-node-cancelButton").onclick = null;
            document.getElementById("edit-node-popUp").style.display = "none";
        } else if (mode == 'addNode') {
            document.getElementById("add-node-saveButton").onclick = null;
            document.getElementById("add-node-cancelButton").onclick = null;
            document.getElementById("add-node-popUp").style.display = "none";
        }
    }

    $scope.cancelNodeEdit = function(callback) {
        $scope.clearNodePopUp('editNode');
        callback(null);
    }
      
    $scope.saveNodeData = function(data, mode, callback) {
        var properties;
        var config = {}
        if (mode == 'editNode') {
            // TODO: call backend to execute cypher query to set/edit the new properties
            properties = $scope.manipulation.editNodeProperties
            config.node_id = data.id
        } else if (mode == 'addNode') {
            // TODO: call backend to execute cypher query to create a new node (and get the new node ID)
            // data.id =    
            properties = $scope.manipulation.addNodeProperties
            data.group = $scope.manipulation.addNodeLabel  // TODO check that it is defined
        }
        config.label = data.group
        config.properties = properties

        $http.post(getWebAppBackendUrl("/set_title"), config)
        .then(function(response) {
            data.title = response.data.title
            if ("caption" in $scope.nodeParams[data.group]) {
                data.label = properties.filter(item => item.key == $scope.nodeParams[data.group].caption)[0].value
            }
            $scope.clearNodePopUp(mode);
            callback(data);
        }, function(e) {
            $scope.error.msg = e.data;
        });
        // $scope.data.nodes.update({id: data.id, label: "NEW node !"})
    }
    
    $scope.editEdgeWithoutDrag = function(data, callback) {
        $scope.manipulation.editEdgePopup = `${data.label} (${data.from} -> ${data.to})`
        
        $http.post(getWebAppBackendUrl("/get_selected_edge_properties"), {src_id: data.from, tgt_id: data.to, rel_type: data.label})
        .then(function(response) {
            $scope.manipulation.editEdgeProperties = response.data.properties;
        }, function(e) {
            $scope.error.msg = e.data;
        });

        // document.getElementById("edge-label").value = data.label;
        document.getElementById("edit-edge-saveButton").onclick = $scope.saveEdgeData.bind(
          this,
          data,
          'editEdge',
          callback
        );
        document.getElementById("edit-edge-cancelButton").onclick = $scope.cancelEdgeEdit.bind(
          this,
          'editEdge',
          callback
        );
        document.getElementById("edit-edge-popUp").style.display = "block";
    }

    $scope.addEdgeWithoutDrag = function(data, callback) {
        // $scope.manipulation.addEdgeType
        $scope.manipulation.addEdgeProperties = [{key: "", value: ""}]
        
        document.getElementById("add-edge-saveButton").onclick = $scope.saveEdgeData.bind(
          this,
          data,
          'addEdge',
          callback
        );
        document.getElementById("add-edge-cancelButton").onclick = $scope.cancelEdgeEdit.bind(
          this,
          'addEdge',
          callback
        );
        document.getElementById("add-edge-popUp").style.display = "block";
    }
      
    $scope.clearEdgePopUp = function(mode) {
        if (mode == 'editEdge') {
            document.getElementById("edit-edge-saveButton").onclick = null;
            document.getElementById("edit-edge-cancelButton").onclick = null;
            document.getElementById("edit-edge-popUp").style.display = "none";
        } else if (mode == 'addEdge') {
            document.getElementById("add-edge-saveButton").onclick = null;
            document.getElementById("add-edge-cancelButton").onclick = null;
            document.getElementById("add-edge-popUp").style.display = "none";
        }
    }

    $scope.cancelEdgeEdit = function(mode, callback) {
        $scope.clearEdgePopUp(mode);
        callback(null);
    }

    $scope.saveEdgeData = function(data, mode, callback) {
        if (typeof data.to === "object") data.to = data.to.id;
        if (typeof data.from === "object") data.from = data.from.id;

        var properties; 
        var config = {}
        if (mode == 'editEdge') {
            // TODO: call backend to execute cypher query to set/edit the new properties
            properties = $scope.manipulation.editEdgeProperties
        } else if (mode == 'addEdge') {
            // TODO: call backend to execute cypher query to create a new edge
            properties = $scope.manipulation.addEdgeProperties
            data.label = $scope.manipulation.addEdgeType
        }
        config.label = data.label
        config.properties = properties

        $http.post(getWebAppBackendUrl("/set_title"), config)
        .then(function(response) {
            data.title = response.data.title
            $scope.clearEdgePopUp(mode);
            callback(data);
        }, function(e) {
            $scope.error.msg = e.data;
        });
    }
    
    $scope.draw = function() {
        if ($scope.template == 'globalView' && $scope.relationships.length == 0) {
            console.warn("Cannot draw graph !")
            return;
        } else if ($scope.template == 'localView') {
            if (!($scope.localSelection.nodeId && $scope.localSelection.nodes && $scope.localSelection.relations)) {
                console.warn("Cannot draw subgraph !")
                return;
            }
        }

        console.log("scope before: ", $scope)

        $scope.error = {}
        document.getElementById('graph-container').innerHTML = ""
        console.log("$scope.error: ", $scope.error)
        $scope.container = document.getElementById('graph-container')
        $scope.options = {
            nodes: {
                shape: "dot",
                size: 20,
                scaling: {
                    min: 10,
                    max: 30
                },
                font: {
                    size: 12,
                    face: "Tahoma",
                    strokeWidth: 7
                }
            },
            
            edges:{
                scaling: {
                    min: 1,
                    max: 5,
                    label: false
                },
                color: { inherit: true },
                // smooth: {
                //     type: "continuous"
                // },
                arrows: {
                    to: {enabled: true}
                },
            },
            interaction: {
                hideEdgesOnDrag: false,
                hideEdgesOnZoom: true,
                tooltipDelay: 200,
                hoverConnectedEdges: true,
                navigationButtons: true,
                keyboard: {
                    enabled: true
                }
            },
            layout: {
                improvedLayout: false,
                hierarchical: {
                    enabled: false,
                    sortMethod: 'hubsize'
                }
            },

            manipulation: {
                addNode: function(data, callback) {
                  console.warn("addNode data: ", data)
                  $scope.addNode(data, $scope.clearNodePopUp, callback);
                },
                editNode: function(data, callback) {
                  console.warn("editNode data: ", data)
                  $scope.editNode(data, $scope.cancelNodeEdit, callback);
                },

                addEdge: function(data, callback) {
                  if (data.from == data.to) {
                    var r = confirm("Do you want to connect the node to itself?");
                    if (r != true) {
                      callback(null);
                      return;
                    }
                  }
                  console.warn("addEdge data: ", data)
                //   document.getElementById("edge-operation").innerHTML = "Add Edge";
                  $scope.addEdgeWithoutDrag(data, callback);
                },
                editEdge: {
                  editWithoutDrag: function(data, callback) {
                    console.warn("editEdge data: ", data)
                    // document.getElementById("edge-operation").innerHTML = "Edit Edge";
                    $scope.editEdgeWithoutDrag(data, callback);
                  }
                },
                deleteNode: function(data, callback) {
                    console.warn("deleteNode data: ", data)
                    callback(data)
                },
                deleteEdge: function(data, callback) {
                    console.warn("deleteEdge data: ", data)
                    callback(data)
                }
            },

            physics: {
                forceAtlas2Based: {
                    gravitationalConstant: -26,
                    centralGravity: 0.005,
                    springLength: 230,
                    springConstant: 0.18
                },
                maxVelocity: 50,
                solver: 'forceAtlas2Based',
                timestep: 0.35,
                stabilization: {
                    iterations: 200,
                    fit: true
                }
            },
            // manipulation: {
            //     editEdge: {
            //       editWithoutDrag: function(data, callback) {
            //         console.info(data);
            //         // alert("The callback data has been logged to the console.");
            //         // you can do something with the data here
            //         callback(data);
            //       }
            //     }
            // }
            // groups: {
            //     useDefaultGroups: true
            // }
            // physics: false
        };
        var relations = []
        if ($scope.template == "globalView") {
            for (var i = 0; i < $scope.relationships.length; i++) {
                var src = $scope.relationships[i].source.split(" - ")
                var tgt = $scope.relationships[i].target.split(" - ")
                var rel = $scope.relationships[i].relation
                var relation = {
                    source: src[0],
                    src_id: `${src[0]}_${src[1]}`,
                    relation: rel,
                    rel_id: `${rel}_${i}`,
                    target: tgt[0],
                    tgt_id: `${tgt[0]}_${tgt[1]}`
                }
                relations.push(relation)            
            }
            console.log("relations: ", relations)
        }
//         var node_params = {}
        
        var node_params = $scope.nodeParams
        for (var node in node_params) {
            node_params[node].color = node
        }
        console.log("node_params: ", node_params)
        var rel_params = {}
        var isSubgraph = ($scope.template == "localView") ? true : false 
        var config = {
            query_limit: $scope.queryLimit,
            relations: relations,
            node_params: node_params,
            rel_params: rel_params,
            local_selection: $scope.localSelection,
            subgraph: isSubgraph
        }

        console.log("config: ", config)
        $http.post(getWebAppBackendUrl("/draw_graph"), config)
//         $http.get(getWebAppBackendUrl("/draw_graph"), {params: {config: JSON.stringify(config)}})
        .then(function(response) {
            $scope.data = {
                nodes: new vis.DataSet(response.data.nodes),
                edges: new vis.DataSet(response.data.edges)
            }
            
            console.log("data: ", $scope.data)

            // console.log("before network: ", $scope.network.body.nodes)
            // var nodes = new vis.DataSet(response.data.nodes);
            // var edges = new vis.DataSet(response.data.edges);
            // $scope.network.setData({ nodes: nodes, edges: edges });
            // $scope.network.setData({ nodes: response.data.nodes, edges: response.data.edges });

            // $scope.network.setData($scope.data)
            // $scope.network.setOptions($scope.options)
            // console.log("after network: ", $scope.network.body.nodes)

            $scope.network = new vis.Network($scope.container, $scope.data, $scope.options);
            stopStabilization($scope.network, 5000);
            console.log("finsih timeout")

            $scope.network.on('doubleClick', function(data) {

                console.log("this: ", this)
                console.log('clicked data:', data);
                // $scope.data.nodes.update({id: data.nodes[0], label: "NEW node !"})
                // var newData = {
                //     nodes: [{ id: 1, label: "Mayeul", color: "#97C2FC" }, {id: 2, label: "Alex C", color: "#FFFF00" }],
                //     edges: [{ from: 1, to: 2}]
                // };
                // this.setData(newData)
            });
        }, function(e) {
            console.log("about to get error: ", e.data)
            $scope.error.msg = e.data
        });
        
    }

    

    function stopStabilization(network, time){
        setTimeout(() => {
                network.stopSimulation();
                console.log("Stabilization stopped !")
            },
            time
        );
    }
})
