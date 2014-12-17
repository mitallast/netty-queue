Queue server
============

This is an simple java queue server.

Goals:

 - REST for simple integration
 - JSON for flexible message structures
 - STOMP for fast and asynchronous api
 - Netty for fastest TCP/IP and HTTP server realization
 - Memory mapped files to access data as an transaction log

As result one instance allows:
 - send up to 165175 REST messages-at-request per second in concurrent mode
 - send up to 176600 STOMP messages-at-request per second in concurrent mode

See integration test benchmarks for more details.

How to run
----------

    mvn exec:exec -DskipTests
    
REST usage
----------

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


STOMP usage
-----------

Connect and send json messages async:

    $ telnet 127.0.0.1 9080
    Trying 127.0.0.1...
    Connected to localhost.
    Escape character is '^]'.
    CONNECT

    ^@
    CONNECTED
    version:1.2

    SEND
    destination:my_queue
    content-type:json
    content-length:26
    receipt:1

    {"message":"Hello world 1"}
    ^@
    SEND
    destination:my_queue
    content-type:json
    content-length:26
    receipt:2

    {"message":"Hello world 2"}
    ^@
    RECEIPT
    receipt-id:1
    RECEIPT
    receipt-id:2

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
    -XX:MaxPermSize=512m
    -XX:ReservedCodeCacheSize=256m
    -XX:+UseG1GC
    -XX:MaxGCPauseMillis=150
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
    -Dio.netty.leakDetectionLevel=disabled
