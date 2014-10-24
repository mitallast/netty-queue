Queue server
============

This is an simple java queue server.

Goals:

 - REST for simple integration and flexible message structures
 - Netty for fastest TCP/IP and HTTP server realization
 - Level DB as storage engine for amazing performance

As result, one instance allows send up to 50000 messages-at-request per second in concurrent mode! 
See benchmark for more details.

How to run
----------

    mvn exec:exec
    
Usage
-----

server info 

    curl -i -s -XGET 'localhost:8080/?pretty'

server stats 

    curl -i -s -XGET 'localhost:8080/_stats?pretty'

create queue 

    curl -i -s -XPUT 'localhost:8080/my_queue?type=string_uid&pretty'

queue stats

    curl -i -s -XGET 'localhost:8080/my_queue/_stats?pretty'

enqueue message

    curl -i -s -XPOST 'localhost:8080/my_queue/message?pretty' -d '{"message":"Hello world 1"}'

enqueue with uid custom_uid message 

    curl -i -s -XPOST 'localhost:8080/my_queue/message?pretty' -d '{"uid":"custom_uid","message":"Hello world custom_uid"}'

peek message

    curl -i -s -XHEAD 'localhost:8080/my_queue/message?pretty'

dequeue message

    curl -i -s -XGET 'localhost:8080/my_queue/message?pretty'


delete queue

    curl -i -s -XDELETE 'localhost:8080/my_queue?reason=test&pretty'


Recommended server options
--------------------------

    localhost$ sysctl net.inet.tcp.msl
    net.inet.tcp.msl: 15000
    localhost$ sudo sysctl -w net.inet.tcp.msl=1000
    net.inet.tcp.msl: 15000 -> 1000

Recommended java options
------------------------

    -server
    -d64
    -Xms512m
    -Xmx512m
    -XX:MaxPermSize=512m
    -XX:ReservedCodeCacheSize=256m
    -XX:+UseG1GC
    -XX:MaxGCPauseMillis=50
    -XX:InitiatingHeapOccupancyPercent=70
    -XX:NewRatio=2
    -XX:SurvivorRatio=8
    -XX:MaxTenuringThreshold=15
    -XX:G1ReservePercent=50
    -XX:G1HeapRegionSize=32m
    -XX:+UseCompressedOops
    -XX:+AggressiveOpts
    -XX:+UseFastAccessorMethods
    -XX:-MaxFDLimit
    -Dio.netty.allocator.numDirectArenas=64