#!/bin/bash

echo "Checking server info"
curl "http://localhost:9001/v1/info"
echo
curl "http://localhost:9002/v1/info"
echo
curl "http://localhost:9003/v1/info"
echo

echo "Inserting keys..."
curl -X POST http://localhost:9001/v1/insert \
  -d '{ "key": "testkey" }' \
  -H 'content-type: application/json'
echo

sleep 1

curl -X POST http://localhost:9002/v1/insert \
  -d '{ "key": "testkey2" }' \
  -H 'content-type: application/json'
echo

sleep 1

curl -X POST http://localhost:9003/v1/insert \
  -d '{ "key": "testkey3" }' \
  -H 'content-type: application/json'
echo

sleep 1

echo "Checking existence on node1..."
curl "http://localhost:9001/v1/exists?key=testkey"
echo

sleep 1

curl "http://localhost:9001/v1/exists?key=testkey2"
echo

sleep 1

curl "http://localhost:9001/v1/exists?key=testkey3"
echo

sleep 1

echo "Checking existence on node2..."

curl "http://localhost:9002/v1/exists?key=testkey"
echo

sleep 1

curl "http://localhost:9002/v1/exists?key=testkey2"
echo

sleep 1

curl "http://localhost:9002/v1/exists?key=testkey3"
echo

sleep 1

echo "Checking existence on node3..."

curl "http://localhost:9003/v1/exists?key=testkey"
echo

sleep 1

curl "http://localhost:9003/v1/exists?key=testkey3"
echo

sleep 1

curl "http://localhost:9003/v1/exists?key=testkey3"
echo