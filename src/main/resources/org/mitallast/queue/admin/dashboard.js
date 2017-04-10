(function(){
    'strict';

    angular.module('admin', ['ngRoute', 'ngWebSocket'])
    .config(['$routeProvider', '$locationProvider', function($routeProvider, $locationProvider){
        $locationProvider.html5Mode(false);
        $locationProvider.hashPrefix("");
        $routeProvider
            .when('/', {
                redirectTo: '/settings'
            })
            .when('/settings', {
                templateUrl: '/settings.html',
                controller: 'SettingsCtrl'
            })
            .when('/raft/state', {
                templateUrl: '/raft/state.html',
                controller: 'RaftStateCtrl'
            })
            .when('/raft/log', {
                templateUrl: '/raft/log.html',
                controller: 'RaftLogCtrl'
            })
            .when('/crdt/create', {
                templateUrl: '/crdt/create.html',
                controller: 'CrdtCreateCtrl'
            })
            .when('/crdt/routing', {
                templateUrl: '/crdt/routing.html',
                controller: 'CrdtRoutingCtrl'
            })
            .when('/crdt/:id/update', {
                templateUrl: '/crdt/update.html',
                controller: 'CrdtUpdateCtrl'
            })
            .when('/crdt/:id/value', {
                templateUrl: '/crdt/value.html',
                controller: 'CrdtValueCtrl'
            })
            .otherwise({
                redirectTo: '/'
            });
    }])
    .controller('SidebarCtrl', function($scope, $location){
        $scope.menus = [
            [
                {href:'settings', title:'Settings'},
            ],
            [
                {href:'raft/state', title:'Raft State'},
                {href:'raft/log', title:'Raft Log'},
            ],
            [
                {href:'crdt/routing', title:'Crdt Routing'},
                {href:'crdt/create', title:'Crdt Create'},
                {href:'crdt/0/update', title:'Crdt 0 Update'},
                {href:'crdt/0/value', title:'Crdt 0 Value'},
            ],
        ];
        $scope.activeClass = function(page){
            var current = $location.path().substring(1);
            return page === current ? "active" : "";
        };
    })
    .controller('SettingsCtrl', function($scope, $http){
        $http.get('/_settings')
        .then(function(response){
            $scope.settings = response.data;
        });
    })
    .controller('RaftStateCtrl', function($scope, $http){
        $http.get('/_raft/state')
        .then(function(response){
            $scope.state = response.data;
        });
    })
    .controller('RaftLogCtrl', function($scope, $http){
        $scope.committed = function(entry){
            return entry.index <= $scope.log.committedIndex;
        };
        $http.get('/_raft/log')
        .then(function(response){
            $scope.log = response.data;
        });
    })
    .controller('CrdtRoutingCtrl', function($scope, $http){
        $scope.routing = {}
        $http.get('/_crdt/routing')
        .then(function(response){
            $scope.routing = response.data;
        })
    })
    .controller('CrdtCreateCtrl', function($scope, $http, $location){
        $scope.id = 0;
        $scope.create = function() {
            $http.put('/_crdt/' + $scope.id + '/lww-register', $scope.dag)
            .then(
                function(response){
                    $location.path('/crdt/' + $scope.id + '/value')
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
    })
    .controller('CrdtUpdateCtrl', function($scope, $http, $routeParams){
        $scope.id = parseInt($routeParams.id);
        $scope.value = '{}'
        $scope.update = function() {
            $http.put('/_crdt/' + $scope.id + '/lww-register/value', $scope.value)
            .then(
                function(response){
                    $location.path('/crdt/' + $scope.id + '/value')
                },
                function(response){
                    $scope.errors = response.data;
                }
            );
        };
    })
    .controller('CrdtValueCtrl', function($scope, $http, $routeParams){
        $scope.id = parseInt($routeParams.id);
        $scope.value = null;
        $http.get('/_crdt/' + $scope.id + '/lww-register/value')
        .then(
            function(response){
                $scope.value = response.data;
            },
            function(response){
                $scope.errors = response.data
            }
        );
    })

    function humanizeBytes(bytes) {
        var units = ["B/s","KB/s","MB/s","GB/s","TB/s","PB/s"];
        var unit = 0;
        while(true) {
            if(unit >= units.length) {
                break;
            }
            if(bytes > 1024){
                bytes = bytes / 1024;
                unit = unit + 1;
            }else{
                break;
            }
        }
        return bytes.toFixed(3) + " " + units[unit];
    }
})();