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
    localhost$ ulimit -n 65536
    localhost$ ulimit -n
    65536

Recommended java options
------------------------

    -server
    -d64
    -Xms4g
    -Xmx4g
    -Djava.net.preferIPv4Stack=true
    -XX:MaxDirectMemorySize=1g
    -XX:+UseParNewGC
    -XX:+UseConcMarkSweepGC
    -XX:CMSInitiatingOccupancyFraction=75
    -XX:+UseCMSInitiatingOccupancyOnly
    -XX:MaxGCPauseMillis=150
    -XX:InitiatingHeapOccupancyPercent=70
    -XX:NewRatio=2
    -XX:SurvivorRatio=8
    -XX:MaxTenuringThreshold=15
    -XX:+UseCompressedOops
    -XX:+AggressiveOpts
    -XX:+UseFastAccessorMethods
    -XX:-MaxFDLimit
    -Dio.netty.leakDetectionLevel=disabled
