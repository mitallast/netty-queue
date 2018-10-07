Queue server
============

This is an simple java queue server.

Goals:

 - REST for simple integration
 - JSON for flexible message structures
 - Netty for fastest TCP/IP and HTTP server realization
 - Raft for safe cluster consensus 

As result one instance allows:
 - send up to 160000 REST message-at-request per second in concurrent mode
 - send up to 600000 binary protocol message-at-request per second in concurrent mode

See integration test benchmarks for more details.

See tests for communication example

Recommended server options
--------------------------

    localhost$ sysctl net.inet.tcp.msl
    net.inet.tcp.msl: 15000
    localhost$ sudo sysctl -w net.inet.tcp.msl=1000
    net.inet.tcp.msl: 15000 -> 1000
    localhost$ ulimit -n
    2048
    localhost$ sudo ulimit -n 65536
    localhost$ ulimit -n
    65536

Recommended java options
------------------------

    -server
    -Xms4g
    -Xmx4g
    -Xss6m
    -XX:+AggressiveOpts
    -XX:+UseCompressedOops
    -XX:-MaxFDLimit
    -XX:+AlwaysPreTouch
    -XX:+DisableExplicitGC
    -XX:+TieredCompilation
    -XX:+UnlockDiagnosticVMOptions
    -XX:+UnlockExperimentalVMOptions
    -XX:InitialRAMFraction=1
    -XX:MaxRAMFraction=1
    -XX:MinRAMFraction=1
    -XX:+UseAES
    -XX:+UseAESIntrinsics
    -XX:+UseG1GC
    -XX:+UseStringDeduplication
    -XX:-UseBiasedLocking
    -XX:ConcGCThreads=5
    -XX:G1HeapRegionSize=16m
    -XX:G1MaxNewSizePercent=80
    -XX:G1MixedGCLiveThresholdPercent=50
    -XX:G1NewSizePercent=50
    -XX:InitiatingHeapOccupancyPercent=10
    -XX:MaxGCPauseMillis=100
    -XX:NewSize=512m
    -XX:ParallelGCThreads=20
    -XX:ReservedCodeCacheSize=256m
    -XX:TargetSurvivorRatio=90
    -Dio.netty.leakDetectionLevel=disabled
    -Djava.net.preferIPv4Stack=true