#!/bin/sh

echo info
curl -s -XGET localhost:8080/
echo

echo stats
curl -s -XGET localhost:8080/_stats
echo

echo create queue
curl -s -XPUT localhost:8080/my_queue?type=string
echo

echo stats
curl -s -XGET localhost:8080/_stats
echo

echo enqueue
curl -s -XPUT localhost:8080/my_queue/_enqueue -d 'Hello world'
echo

echo dequeue
curl -s -XGET localhost:8080/my_queue/_dequeue
echo

echo dequeue
curl -s -XGET localhost:8080/my_queue/_dequeue
echo

#ab -n100000 -c100 -k -r -p README.md localhost:8080/my_queue/_enqueue
#ab -n100000 -c100 -k -r localhost:8080/

echo delete queue
curl -s -XDELETE localhost:8080/my_queue?reason=test
echo

echo stats
curl -s -XGET localhost:8080/_stats
echo