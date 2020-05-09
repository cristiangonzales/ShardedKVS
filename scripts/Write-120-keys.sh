#!/bin/sh
# Write 120 values to the network with 8 nodes

# Prerequisite: Docker & Brew (or alternatively, `pwgen`)

# Check to see if pwgen is installed on the user's machine
if ! brew info pwgen &>/dev/null; then
	brew install pwgen
fi

# Port numbers that represent four nodes
PORT0=8080

# Iterations to be done
upper_bound=120
# Array of keys to be kept as we make requests to write to them
key_arr=()

# For loop to generate random keys of length 0 to 100 and append them to the key array 
for (( i=0; i<$upper_bound; i++))
do
	# Generate a key of fixed length 15
	KEY=$(pwgen 15 1)
	echo Key to be written to $PORT0: $KEY
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
	curl -X PUT http://localhost:$PORT0/kvs -d "key=$i&value=$VAL&causal_payload="
	echo
done
