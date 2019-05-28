var app = angular.module('neo4j.module', []);

app.controller('Neo4jController', function($scope) {

    // Use Python backend to get nodes list
    var updateNodes = function() {
        $scope.callPythonDo({}).then(function(data) {
            // success
            console.warn('data', data)
            $scope.nodes = data.nodes;
        }, function(data) {
            // failure
            $scope.nodes = [];
        });
    };
    updateNodes();
    $scope.$watch('config.nodeType', updateNodes);

});