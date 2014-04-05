

Server options:

    localhost$ sysctl net.inet.tcp.msl
    net.inet.tcp.msl: 15000
    localhost$ sudo sysctl -w net.inet.tcp.msl=1000
    net.inet.tcp.msl: 15000 -> 1000

Java options:

    -server
    -d64
    -Xms2048m
    -Xmx2048m
    -XX:MaxPermSize=512m
    -XX:ReservedCodeCacheSize=256m
    -XX:+UseG1GC
    -XX:MaxGCPauseMillis=50
    -XX:InitiatingHeapOccupancyPercent=30
    -XX:NewRatio=2
    -XX:SurvivorRatio=8
    -XX:MaxTenuringThreshold=15
    -XX:ParallelGCThreads=8
    -XX:ConcGCThreads=8
    -XX:G1ReservePercent=50
    -XX:G1HeapRegionSize=32m
    -XX:+UseCompressedOops
    -XX:+AggressiveOpts
    -XX:+UseFastAccessorMethods
    -XX:CompileThreshold=100
    -XX:-MaxFDLimit
    -Dio.netty.allocator.numDirectArenas=64