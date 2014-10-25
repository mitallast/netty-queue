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
curl -i -s -XPUT 'localhost:8080/my_queue?type=string_uid&pretty'
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

echo my_queue enqueue uid custom_uid
echo
curl -i -s -XPOST 'localhost:8080/my_queue/message?pretty' -d '{"uuid":"a57586b7-3eed-4c7c-b257-8bf9021fffbd","message":"Hello world custom_uid"}'
echo
echo

echo my_queue enqueue uid custom_uid fail
echo
curl -i -s -XPOST 'localhost:8080/my_queue/message?pretty' -d '{"uuid":"a57586b7-3eed-4c7c-b257-8bf9021fffbd","message":"Hello world custom_uid"}'
echo
echo

#ab -n100000 -c100 -k -p message.json 'localhost:8080/my_queue/message'
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