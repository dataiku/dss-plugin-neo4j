var app = angular.module("graphApp", []);

app.controller("GraphController", function($scope, $http) {
    $scope.template = "globalView"
    // initialize first row of select
    $scope.error = {}
    $scope.relationships = []
    $scope.nodeProperties = {}
    $scope.nodeParams = {}
    
    $scope.uniqueRelations = new Set()
    $scope.selectedRelationship = {
        source: "Transfer",
        relation: "OF_PLAYER",
        target: "Player"
    }
    $scope.queryLimit = 20
    $scope.nodeLabels = [] 
    $scope.usedNodeIndex = {}
    
    // params for local view
    $scope.localSelection = {
    }

    console.warn("$scope.selectedRelationship: ", $scope.selectedRelationship)

    $scope.setTemplate = function(template) {
        if ($scope.template != template) {
            $scope.template = template
        }
        // console.log("Child $scope: ", $scope)
        // console.log("selectedRelationship: ", $scope.selectedRelationship)
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
            if (!(source_label in $scope.nodeProperties)) {
                $http.post(getWebAppBackendUrl("/get_node_properties"), {node_label: source_label})
                .then(function(response) {
                    $scope.nodeProperties[source_label] = {
                        all: response.data.node_properties_all,
                        num: response.data.node_properties_num
                    }
                }, function(e) {
                    $scope.error.msg = e.data;
                });
            }
            var target_label = relationship.target.split(" - ")[0]
            if (!(target_label in $scope.nodeProperties)) {
                $http.post(getWebAppBackendUrl("/get_node_properties"), {node_label: target_label})
                .then(function(response) {
                    $scope.nodeProperties[target_label] = {
                        all: response.data.node_properties_all,
                        num: response.data.node_properties_num
                    }
                }, function(e) {
                    $scope.error.msg = e.data;
                });
            }

            // $scope.selectedRelationship = {
            //     source: "",
            //     relation: "",
            //     target: ""
            // }
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
            delete $scope.nodeProperties[source_label]
        }
        if ($scope.usedNodeIndex[target_label] == 0) {
            delete $scope.nodeProperties[target_label]
        }
        console.log("$scope.nodeProperties: ", $scope.nodeProperties)
    }
    
    $scope.reset = function() {
       document.location.reload(true);
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
                nodes: response.data.nodes,
                edges: response.data.edges
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
                var newData = {
                    nodes: [{ id: 1, label: "Mayeul", color: "#97C2FC" }, {id: 2, label: "Alex C", color: "#FFFF00" }],
                    edges: [{ from: 1, to: 2}]
                };
                this.setData(newData)
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
