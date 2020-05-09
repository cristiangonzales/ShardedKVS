#!/bin/sh
# Read from all 8 nodes in the network the number of keys that each one has

# Prerequisite: Docker & Brew (or alternatively, `pwgen`)

# Check to see if pwgen is installed on the user's machine
if ! brew info pwgen &>/dev/null; then
	brew install pwgen
fi

# Port numbers that represent four nodes
PORT0=8080
PORT1=8081
PORT2=8082
PORT3=8083
PORT4=8084
PORT5=8085
PORT6=8086

echo Getting number of keys for $PORT0...
curl -X GET http://localhost:$PORT0/kvs/get_number_of_keys 
echo
echo Getting number of keys for $PORT1...
curl -X GET http://localhost:$PORT1/kvs/get_number_of_keys 
echo
echo Getting number of keys for $PORT2...
curl -X GET http://localhost:$PORT2/kvs/get_number_of_keys 
echo
echo Getting number of keys for $PORT3...
curl -X GET http://localhost:$PORT3/kvs/get_number_of_keys 
echo
echo Getting number of keys for $PORT4...
curl -X GET http://localhost:$PORT4/kvs/get_number_of_keys 
echo
echo Getting number of keys for $PORT5...
curl -X GET http://localhost:$PORT5/kvs/get_number_of_keys 
echo
echo Getting number of keys for $PORT6...
curl -X GET http://localhost:$PORT6/kvs/get_number_of_keys 
echo

echo Getting partition ID for $PORT0...
curl -X GET http://localhost:$PORT0/kvs/get_partition_id 
echo
echo Getting partition ID for $PORT1...
curl -X GET http://localhost:$PORT1/kvs/get_partition_id
echo
echo Getting partition ID for $PORT2...
curl -X GET http://localhost:$PORT2/kvs/get_partition_id
echo
echo Getting partition ID for $PORT3...
curl -X GET http://localhost:$PORT3/kvs/get_partition_id
echo
echo Getting partition ID for $PORT4...
curl -X GET http://localhost:$PORT4/kvs/get_partition_id
echo
echo Getting partition ID for $PORT5...
curl -X GET http://localhost:$PORT5/kvs/get_partition_id
echo
echo Getting partition ID for $PORT6...
curl -X GET http://localhost:$PORT6/kvs/get_partition_id
echo

echo Testing '/kvs/get_all_partition_ids' for $PORT0, $PORT1, $PORT2, $PORT3, $PORT4, $PORT5, $PORT6...
curl -X GET http://localhost:$PORT0/kvs/get_all_partition_ids
echo
curl -X GET http://localhost:$PORT1/kvs/get_all_partition_ids
echo
curl -X GET http://localhost:$PORT2/kvs/get_all_partition_ids
echo
curl -X GET http://localhost:$PORT3/kvs/get_all_partition_ids 
echo
curl -X GET http://localhost:$PORT4/kvs/get_all_partition_ids 
echo
curl -X GET http://localhost:$PORT5/kvs/get_all_partition_ids 
echo
curl -X GET http://localhost:$PORT6/kvs/get_all_partition_ids 
echo
