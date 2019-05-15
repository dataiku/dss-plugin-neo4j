var app = angular.module('neo4j.module', []);

app.controller('Neo4jController', function($scope) {
    // Placeholder if we want to add more advanced behavior to the plugin
    //$scope.config.neo4jUri = "bolt://localhost:7687";
    //$scope.config.neo4jUsername = "neo4j";
    //$scope.config.neo4jPassword = "dataiku";
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
    $scope.$watch('config.filterColumn', updateNodes);
});