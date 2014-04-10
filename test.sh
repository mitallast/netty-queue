#!/bin/sh

curl -s -XGET localhost:8080/
echo
curl -s -XGET localhost:8080/_stats
echo
curl -s -XPUT localhost:8080/my_queue?type=string
echo
curl -s -XGET localhost:8080/_stats
echo

curl -s -XPUT localhost:8080/my_queue/_enqueue -d 'Hello world'
echo

ab -n100000 -c100 -k -r -p README.md localhost:8080/my_queue/_enqueue

curl -s -XDELETE localhost:8080/my_queue?reason=test
echo
curl -s -XGET localhost:8080/_stats
echo