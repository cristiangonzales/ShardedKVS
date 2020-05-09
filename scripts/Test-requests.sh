#!/bin/sh
# Emulate a Docker network with 4 nodes


# TODO: Lots of work needs to be done to this script
# CURRENTLY DEPRECATED RELATIVE TO SPEC REQUIREMENTS


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
PORT7=8087

# Iterations to be done
upper_bound=120
# Array of keys to be kept as we make requests to write to them
key_arr=()

# For loop to generate random keys of length 0 to 100 and append them to the key array 
for (( i=0; i<$upper_bound; i++))
do
	# Generate a key of fixed length 15
	KEY=$(pwgen 15 1)
	# Append the key
	key_arr+=($KEY)
done

echo Sending 120 write requests to port $PORT0...
for i in "${key_arr[@]}"
do
	# Generate random value length of 0-20
	val_len=$(( ( RANDOM % 20 )  + 1 ))
	# Generate a random value of the length 'val_len'
	VAL=$(pwgen $val_len 1)
	# Initiate client request
	curl -X PUT http://localhost:$PORT0/kvs -d "key=$i&value=$VAL"
	echo
done

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

echo Sending 400 read requests to port $PORT3...
for i in "${key_arr[@]}"
do
	# Initiate client request
	curl -X GET http://localhost:$PORT3/kvs?key=$i
	echo
done

echo Sending 400 write requests to port $PORT2 with the same keys...
for i in "${key_arr[@]}"
do
	# Generate random value length of 0-20
	val_len=$(( ( RANDOM % 20 )  + 1 ))
	# Generate a random value of the length 'val_len'
	VAL=$(pwgen $val_len 1)
	# Initiate client request
	curl -X PUT http://localhost:$PORT2/kvs -d "key=$i&value=$VAL"
	echo
done

echo Sending 400 read requests to port $PORT4...
for i in "${key_arr[@]}"
do
	# Initiate client request
	curl -X GET http://localhost:$PORT4/kvs?key=$i
	echo
done
