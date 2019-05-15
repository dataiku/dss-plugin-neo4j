var app = angular.module('neo4j.module', []);

app.controller('Neo4jController', function($scope) {
    
    // To remove !!
    $scope.config.neo4jUri = "bolt://localhost:7687";
    $scope.config.neo4jUsername = "neo4j";
    $scope.config.neo4jPassword = "dataiku";
    
    // Use Python backend to get nodes list
    var updateNodes = function() {
        $scope.callPythonDo({}).then(function(data) {
            // success
            $scope.nodes = data.nodes;
        }, function(data) {
            // failure
            $scope.nodes = [];
        });
    };
    updateNodes();
    $scope.$watch('config.nodeType', updateNodes);
    
});