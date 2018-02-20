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
            .when('/audio', {
                templateUrl: '/audio.html',
                controller: 'AudioCtrl'
            })
            .otherwise({
                redirectTo: '/'
            });
    }])
    .factory('$audio', function($websocket){
        var url =
            (location.protocol === "https:" ? "wss://" : "ws://") +
            location.hostname +
            (location.port ? ':' + location.port : '') +
            "/ws/";
        var stream = $websocket(url, null, {binaryType: 'arraybuffer'});
        console.log("stream", stream);
        var consumers = [];
        stream.onMessage(function(frame){
            consumers.forEach(function(consumer){
                consumer(frame);
            });
        });
        return {
            send: function(buffer){
                stream.send(buffer);
            },
            subscribe: function(consumer){
                consumers.push(consumer);
            }
        };
    })
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
            [
                {href:'audio', title:'Audio'},
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
    .controller('AudioCtrl', function($scope, $http, $audio, $routeParams){
        console.log("AudioCtrl")
        var audioContext = new AudioContext();

        var mixer = audioContext.createGain();

        var audioInLevel = audioContext.createGain();
        audioInLevel.gain.value = 1;
        audioInLevel.connect(mixer);

        // loop back
        // mixer.connect(audioContext.destination);

        var audioIn;

        var onGotDevices = function(devInfos) {
            var index = 0;
            for (var i = 0; i < devInfos.length; i++) {
                var info = devInfos[i];
                if (info.kind !== 'audioinput') {
                    continue;
                }
                var name = info.label || ("Audio in " + (++index));
                console.log("use " + name, info);
                onChangeAudioIn(info.deviceId)
                break;
            }
        };

        var onGotAudioIn = function(stream) {
            if (audioIn != null) {
              audioIn.disconnect();
            }
            audioIn = audioContext.createMediaStreamSource(stream);
            audioIn.connect(audioInLevel);
        };

        var onChangeAudioIn = function(deviceId) {
            var constraint;
            if (navigator.webkitGetUserMedia !== undefined) {
                constraint = {
                    video: false,
                    audio: {
                        optional: [
                            {sourceId:deviceId},
                            {googAutoGainControl: false},
                            {googAutoGainControl2: false},
                            {echoCancellation: false},
                            {googEchoCancellation: false},
                            {googEchoCancellation2: false},
                            {googDAEchoCancellation: false},
                            {googNoiseSuppression: false},
                            {googNoiseSuppression2: false},
                            {googHighpassFilter: false},
                            {googTypingNoiseDetection: false},
                            {googAudioMirroring: false}
                        ]
                    }
                }
            }
            else if (navigator.mozGetUserMedia !== undefined) {
                constraint = {
                    video: false,
                    audio: {
                        deviceId: deviceId ? { exact: deviceId } : void 0,
                        echoCancellation: false,
                        mozAutoGainControl: false
                    }
                }
            }
            else {
                constraint = {
                    video: false,
                    audio: {
                        deviceId: deviceId ? {exact: deviceId} : void 0,
                        echoCancellation: false
                    }
                }
            }
            if ((navigator.mediaDevices != null) && (navigator.mediaDevices.getUserMedia != null)) {
                navigator.mediaDevices.getUserMedia(constraint)
                    .then(onGotAudioIn)["catch"](function(err) {
                        console.log(err);
                    });
            } else {
                navigator.getUserMedia(constraint, onGotAudioIn, function() {
                    console.log(err);
                });
            }
        };

        if ((navigator.mediaDevices != null) && (navigator.mediaDevices.enumerateDevices != null)) {
            navigator.mediaDevices.enumerateDevices().then(onGotDevices)["catch"](function(err) {
              return console.log("Could not enumerate audio devices: " + err);
            });
        } else {
            console.log("no enumerate devices")
        }

        console.log("mixer", mixer);

        if(audioContext.createScriptProcessor==null){
            audioContext.createScriptProcessor=audioContext.createJavaScriptNode;
        }

        var processor=audioContext.createScriptProcessor(16384, 1, 1);
        var input=audioContext.createGain();
        mixer.connect(input);
        input.connect(processor);

        processor.connect(audioContext.destination);

        processor.onaudioprocess = function(e){
            var samples = e.inputBuffer.length * 8820 / e.inputBuffer.sampleRate;
            var offlineContext = new OfflineAudioContext(1, samples, 8820);
            var bufferSource = offlineContext.createBufferSource();
            bufferSource.buffer = e.inputBuffer;
            bufferSource.connect(offlineContext.destination);
            bufferSource.start(0);

            offlineContext.startRendering().then(function(renderedBuffer){
                $audio.send(renderedBuffer.getChannelData(0))
            });
        };

        var startTime = 0;
        $audio.subscribe(function(frame){
            if(!window.lastFrame){
                window.lastFrame = frame;
            }
            var floatBuffer = new Float32Array(frame.data);
            var audioBuffer = audioContext.createBuffer(1, floatBuffer.length, 8820);
            audioBuffer.getChannelData(0).set(floatBuffer);

            var source = audioContext.createBufferSource();
            source.buffer = audioBuffer;
            source.connect(audioContext.destination);
            source.start();
            startTime += audioBuffer.duration;
        })
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