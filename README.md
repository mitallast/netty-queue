Queue server
============

This is an simple java queue server.

Goals:

 - REST for simple integration and flexible message structures
 - Netty for fastest TCP/IP and HTTP server realization
 - Memory mapped files to access data as an transaction log

As result, one instance allows send up to 85000 messages-at-request per second in concurrent mode! 
See benchmark for more details.

How to run
----------

    mvn exec:exec
    
Usage
-----

server info 

    curl -i -s -XGET 'http://127.0.0.1:8080/?pretty'

server stats 

    curl -i -s -XGET 'http://127.0.0.1:8080/_stats?pretty'

create queue 

    curl -i -s -XPUT 'http://127.0.0.1:8080/my_queue?pretty'

queue stats

    curl -i -s -XGET 'http://127.0.0.1:8080/my_queue/_stats?pretty'

enqueue message as string

    curl -i -s -XPOST 'http://127.0.0.1:8080/my_queue/message?pretty' -d '{"message":"Hello world 1"}'

enqueue message as json

    curl -i -s -XPOST 'http://127.0.0.1:8080/my_queue/message?pretty' -d '{"message":{"Title":"Hello world"}}'

enqueue with uuid message 

    curl -i -s -XPOST 'http://127.0.0.1:8080/my_queue/message?pretty' -d '{"uuid":"a57586b7-3eed-4c7c-b257-8bf9021fffbd","message":"Hello world custom_uid"}'

delete with uuid

    curl -i -s -XDELETE 'http://127.0.0.1:8080/my_queue/message?uuid=a57586b7-3eed-4c7c-b257-8bf9021fffbd&pretty'
    curl -i -s -XDELETE 'http://127.0.0.1:8080/my_queue/message/a57586b7-3eed-4c7c-b257-8bf9021fffbd?pretty'

peek message with uuid

    curl -i -s -XHEAD 'http://127.0.0.1:8080/my_queue/message?pretty'

get message with uuid

    curl -i -s -XHEAD 'http://127.0.0.1:8080/my_queue/message/a57586b7-3eed-4c7c-b256-8bf9021fffbd?pretty'

dequeue message

    curl -i -s -XGET 'http://127.0.0.1:8080/my_queue/message?pretty'


delete queue

    curl -i -s -XDELETE 'http://127.0.0.1:8080/my_queue?reason=test&pretty'


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
