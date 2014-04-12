#!/bin/sh

echo info
echo
curl -i -s -XGET 'localhost:8080/?pretty'
echo
echo

echo stats
echo
curl -i -s -XGET 'localhost:8080/_stats?pretty'
echo
echo

echo create queue
echo
curl -i -s -XPUT 'localhost:8080/my_queue?type=string&pretty'
echo
echo

echo stats
echo
curl -i -s -XGET 'localhost:8080/_stats?pretty'
echo
echo

echo my_queue stats
echo
curl -i -s -XGET 'localhost:8080/my_queue/_stats?pretty'
echo
echo

echo enqueue
echo
curl -i -s -XPOST 'localhost:8080/my_queue/message?pretty' -d '{"message":"Hello world 1"}'
echo
echo

echo enqueue
echo
curl -i -s -XPOST 'localhost:8080/my_queue/message?pretty' -d '{"message":"Hello world 2"}'
echo
echo

echo my_queue stats
echo
curl -i -s -XGET 'localhost:8080/my_queue/_stats?pretty'
echo
echo

echo peek
echo
curl -i -s -XHEAD 'localhost:8080/my_queue/message?pretty'
echo
echo

echo my_queue stats
echo
curl -i -s -XGET 'localhost:8080/my_queue/_stats?pretty'
echo
echo

echo dequeue
echo
curl -i -s -XGET 'localhost:8080/my_queue/message?pretty'
echo
echo

echo my_queue stats
echo
curl -i -s -XGET 'localhost:8080/my_queue/_stats?pretty'
echo
echo

echo peek
echo
curl -i -s -XHEAD 'localhost:8080/my_queue/message?pretty'
echo
echo

echo my_queue stats
echo
curl -i -s -XGET 'localhost:8080/my_queue/_stats?pretty'
echo
echo

echo dequeue
echo
curl -i -s -XGET 'localhost:8080/my_queue/message?pretty'
echo
echo

echo my_queue stats
echo
curl -i -s -XGET 'localhost:8080/my_queue/_stats?pretty'
echo
echo

#ab -n100000 -c100 -k -r -p README.md 'localhost:8080/my_queue/_enqueue
#ab -n100000 -c100 -k -r 'localhost:8080/

echo delete queue
echo
curl -i -s -XDELETE 'localhost:8080/my_queue?reason=test&pretty'
echo
echo

echo stats
echo
curl -i -s -XGET 'localhost:8080/_stats?pretty'
echo