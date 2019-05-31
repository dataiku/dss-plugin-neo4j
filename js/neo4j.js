const app = angular.module('neo4j.module', []);

app.controller('Neo4jController', function($scope) {
    function updateNodes() {
        $scope.callPythonDo({}).then(function(data) {
            $scope.nodes = data.nodes;
        }, function(data) {
            $scope.nodes = [];
        });
    };
    updateNodes();
    $scope.$watch('config.nodeType', updateNodes);
});