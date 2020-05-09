"""
    Author: Cristian Gonzales
"""

import logging

import hashlib
from math import ceil
import time

from flask import Response
from flask import request
from flask import abort
from flask_restful import Resource

import json
import requests
import re
import globals

"""
   Route that services reads, writes, and deletes to this process' KVS
"""
class ShardedKVS(Resource):
    """
       GET request
       :return: HTTP response
    """
    def get(self):
        try:
            # Key passed in HTTP body by client
            key = request.args['key']
            # Detecting if there is a network change
            network_change = self.node_health_check()
            # If there is a network change, then we start the iteration of passing a dictionary around to have values
            # appended to it accordingly
            if network_change:
                # If there is a network change, reinitialize your own ranges by calculating the new number of partitions
                num_of_partitions = ceil(len(globals.live_nodes) / globals.value_of_k)
                # Initialize the initial amount of ranges and IDs
                self.init_ranges(num_of_partitions)
                # If we are the only node in the network, we do not need to do read repair
                if len(globals.live_nodes) > 1:
                    # Now, initiate the read repair amongst all the nodes in the case that we have more nodes in our
                    # view of the network
                    self.read_repair()
            # Hash value for key
            key_hash = self.init_hash(key)
            # If the key hash is in the range for this local node's assigned range, then complete the request. Else,
            # forward the request to the appropriate range
            if key_hash in self.my_range():
                # If the hash for the requested key is in our range, and it is a valid key, then it *must* be in our
                # dictionary. If it isn't, then it is an invalid request for a key that does not exist by the client
                if key in globals.KVSDict:
                    # Finally, return the appropriate response to the client
                    return Response(
                        json.dumps({
                            'msg': 'success',
                            'value': globals.KVSDict[key]['value'],
                            'partition_id': self.my_partition_id(),
                            'causal_payload': globals.KVSDict[key]['vector_clock'],
                            'timestamp': globals.KVSDict[key]['timestamp']
                        }),
                        status=201,
                        mimetype='application/json'
                    )
                # In the case that the key is not in our dictionary but does not hash to us *must* mean that the
                # key simply does not exist
                else:
                    return Response(
                        json.dumps({
                            'msg': 'error',
                            'error': 'key does not exist in the kvs'
                        }),
                        status=404,
                        mimetype='application/json'
                    )
            # If we are the only node in our view of the live network, then return an error to the client
            elif len(globals.live_nodes) == 1:
                return Response(
                    json.dumps({
                        'msg': 'error',
                        'error': 'key does not exist in the kvs'
                    }),
                    status=404,
                    mimetype='application/json'
                )
            # Forward the request to the appropriate data partition of nodes
            else:
                # Get the "intended nodes" to send the request to
                intended_nodes = self.get_intended_nodes(key_hash)
                # Return the response from the first reachable node
                for node in intended_nodes:
                    # Forward the appropriate request
                    response = requests.get(
                        "http://" + str(node) + "/kvs/quick_read?key=" + key,
                        timeout=0.1
                    )
                    # Return the response to the client
                    return Response(
                        json.dumps(response.json()),
                        status=int(response.status_code),
                        mimetype='application/json'
                    )
        except Exception as e:
            logging.error(e)
            abort(404, message=str(e))

    """
        PUT request
        :return: HTTP response
    """
    def put(self):
        try:
            # Form data submitted by client
            key = request.form['key']
            # Value for the key
            value = request.form['value']
            # Causal payload submitted by the client
            try:
                causal_payload = request.form['causal_payload']
                # If the causal_payload is the empty string, then simply make it equal to None
                if causal_payload == "":
                    causal_payload = None
            except:
                causal_payload = None
            # Hash value for key
            key_hash = self.init_hash(key)
            # Get the intended node list for the key hash
            intended_nodes = self.get_intended_nodes(key_hash)
            # Conditional to check if the input value is nothing or if there are just no arguments
            if value == None:
                return Response(
                    json.dumps({'msg': 'error', 'error': 'no value provided'}),
                    status=404,
                    mimetype='application/json'
                )

            # Conditional to check if the key is too long (> 250 chars)
            if len(key) > 250:
                return Response(
                    json.dumps({'msg': 'error', 'error': 'key too long'}),
                    status=404,
                    mimetype='application/json'
                )

            # Conditional to check if input is greater than 1.5MB
            if len(value) > 1500000:
                return Response(
                    json.dumps({'msg': 'error', 'error': 'object too large'}),
                    status=404,
                    mimetype='application/json'
                )
            # Conditional to check if the input contains invalid characters outside of [a-zA-Z0-9_]
            if not re.compile('[A-Za-z0-9_]').findall(value):
                return Response(
                    json.dumps({'msg': 'error', 'error': 'key not valid'}),
                    status=404,
                    mimetype='application/json'
                )
            # If the key hash is in the range for this local node's assigned range, then complete the request. Else,
            # forward the request to the appropriate range
            if globals.localIPPort in intended_nodes:
                # In the case that the user does not pass a causal payload, we do not need to do the comparisons for
                # the casual payload given to us and their causal payload
                if causal_payload is not None:
                    # Now, we get the state of this node's VCDict
                    nodes_vector_clock = self.construct_vector_clock()
                    # Check the causal payload with the existing vector clock value for this key
                    vc_flag = self.compare_vector_clocks(nodes_vector_clock, causal_payload)
                    # This is the case where the client's vector clock wins
                    if vc_flag is False:
                        # Map the incoming causal payload to our VCDict, since their clock won
                        self.map_client_vc(causal_payload)
                    # This is the case that they are concurrent
                    elif vc_flag is None:
                        # Map the incoming causal payload to our VCDict, or as many values as we can since they
                        # are not equal
                        self.map_client_vc_concurrent(causal_payload)
                    # In the case that it is True, we can just regularly increment. We only need to do special mapping
                    # if the client's vector clock value is greater, or if they are concurrent.

                # We must increment our own vector clock, and increment everyone else's appropriately
                self.increment_vector_clock(globals.localIPPort)
                # Construct the key-value, along with the appropriate vector clock value and timestamp
                globals.KVSDict[key] = {
                    'value': value,
                    'vector_clock': self.construct_vector_clock(),
                    'timestamp': time.time()
                }
                # Now that we have written this value, we must pass this information down the pipeline to
                # the rest of our replicas. In the case that the replicas are dead for whatever reason,
                # we will wrap the HTTP request in a try-except and continue onto the next one. For the
                # case of "dumb" writes, this is OK for eventual consistency that we try to write as many
                # replicas (or who we think are replicas, on writes) as we can
                for node in intended_nodes:
                    # Don't ping yourself
                    if node == globals.localIPPort:
                        continue
                    # We will try to write to who we think our replica is. If they are "dead" to us, simply
                    # pass
                    try:
                        # Forward the request
                        requests.put(
                            "http://" + str(node) + "/kvs/my_replica_write",
                            data={'key': key, 'data': json.dumps(globals.KVSDict[key])},
                            timeout=0.1
                        )
                    except:
                        pass
                # Return the response
                return Response(
                    json.dumps({
                        'msg': 'success',
                        'partition_id': self.my_partition_id(),
                        'causal_payload': globals.KVSDict[key]['vector_clock'],
                        'timestamp': globals.KVSDict[key]['timestamp']
                    }),
                    status=201,
                    mimetype='application/json'
                )
            # Forward the request to the appropriate IP_PORT value that contains its range
            else:
                # Initial response
                response = None
                # For all node in the "intended_nodes" list, try to write to their KV-stores. In the case that
                # they are unreachable, then simply
                for intended_node in intended_nodes:
                    try:
                        # Forward the request
                        response = requests.put(
                            "http://" + str(intended_node) + "/kvs/data_partition_write",
                            data={
                                'key': key,
                                'value': value,
                                'payload': causal_payload,
                                'caller': globals.localIPPort
                            },
                            timeout=0.5
                        )
                        # Increment this reachable node's value for YOUR dictionary ONLY
                        try:
                            globals.VCDict[intended_node] = globals.VCDict[intended_node] + 1
                        # In the case that it doesn't exist for whatever reason, then we will initialize it to be one in
                        # our dictionary
                        except:
                            globals.VCDict[intended_node] = 1
                        # Once we reach the first available node, and they perform the delegation, then immediately
                        # return the forwarded response back to the client, breaking out of the loop
                        return Response(
                            json.dumps(response.json()),
                            status=int(response.status_code),
                            mimetype='application/json'
                        )
                    except:
                        logging.debug("Response from intended nodes failed")
                        pass
                # In the case that the response is None, then none of the nodes were reachable, so we will simply
                # write it to ourselves
                if response is None:
                    # Increment the vector clock for your dictionary
                    self.increment_vector_clock(globals.localIPPort)
                    # Add the key-val pair to the dictionary
                    globals.KVSDict[key] = {
                        'value': value,
                        'vector_clock': self.construct_vector_clock(),
                        'timestamp': time.time()
                    }
                    # Send the data down the pipeline and write this value to the rest of your replicas
                    for node in self.nodes_in_my_partition():
                        # Don't ping yourself
                        if node == globals.localIPPort:
                            continue
                        # We will try to write to who we think our replica is. If they are "dead" to us, simply
                        # pass
                        try:
                            # Forward the request
                            requests.put(
                                "http://" + str(node) + "/kvs/my_replica_write",
                                data={'key': key, 'data': json.dumps(globals.KVSDict[key])},
                                timeout=0.1
                            )
                        except:
                            pass
        except Exception as e:
            logging.error(e)
            abort(404, message=str(e))

    """
        Helper function to check which nodes in the network are dead to your view, and which are alive. In the case that
        there is a change, a flag will be returned to the caller and this will indicate that there was indeed a change
        in this node's view of the network. We will also get everyone's view list, append their values to ours, eliminate
        duplicates, and lexigraphically sort it (this is in the case of a network heal and they have added nodes that
        we do not know about).
        :return network_change (Boolean)
    """
    def node_health_check(self):
        # Flag to determine if we need to redistribute values and ranges
        network_change = False
        # List that is equal to our current view list, but we will append new nodes accordingly
        temp_list = []
        temp_list.extend(globals.viewList)
        # Iterate through all IP:PORT values in the view list and ping them accordingly, in a for loop
        for ip_port in globals.viewList:
            # Don't ping yourself, and continue onto the next iteration
            if ip_port == globals.localIPPort:
                continue
            # If not, then make the request to the next node and catch the ConnectionError in the case that it is
            # unreachable or dead.
            try:
                # This will return the view_list for the node that we are attempting to reach
                resp = requests.get(
                    "http://" + str(ip_port) + "/kvs/is_node_alive",
                    timeout=0.1
                )
                # Extrapolate the JSON response (their view list) and append it to ours
                node_view_list = resp.json()['success']
                temp_list.extend(node_view_list)
                # If the request is successful and does not throw a connection error, then the following code will be
                # reachable and we know this node is alive
                # The case that a "dead" or unreachable node comes back to life
                if ip_port in globals.dead_nodes:
                    # Make the 'redistribute' flag true so that we can redistribute
                    # data accordingly
                    logging.debug(
                        " * Server " + globals.localIPPort + " reporting server " + ip_port + " to be alive again.")
                    # Removing a node from the dead nodes list, and appending back to life
                    try:
                        globals.dead_nodes.remove(ip_port)
                    except ValueError:
                        pass
                    globals.live_nodes.append(ip_port)
                    # Change the broadcast flag, detecting a network change
                    network_change = True
                # Simple debugging statement to verify that this server is still alive.
                else:
                    logging.debug(
                        " * Server " + globals.localIPPort + " reporting server " + ip_port + " is still alive.")
            except:
                # A formerly live node is now dead, with the caveat that the live node might already be dead
                # (we add a conditional for this reason)
                if ip_port in globals.live_nodes:
                    logging.debug("* Server " + globals.localIPPort + " reporting server " + ip_port + " is NOW dead.")
                    try:
                        globals.live_nodes.remove(ip_port)
                    except ValueError:
                        pass
                    # Append the node to the dead list and flip the flag to True
                    globals.dead_nodes.append(ip_port)
                    network_change = True
                # The case that the node was already dead...
                else:
                    logging.debug("* Server " + globals.localIPPort + " reporting server " + ip_port + " is STILL dead.")
        # If there is indeed a network change, ping the nodes in the set difference between our temp list and our
        # current view list, and see if they're alive
        if network_change:
            # Lexicographically sort our temporary view_list and eliminate duplicates
            temp_list = list(set((temp_list)))
            # New nodes to ping in the set difference between the "temp_list" and the view list
            new_nodes = sorted(list(set(temp_list) - set(globals.viewList)))
            # For each IPPORT value in the sorted list of the set difference between our compiled list of nodes and
            # our current view list, and ping them to see if they are alive
            for ip_port in new_nodes:
                try:
                    requests.get(
                        "http://" + str(ip_port) + "/kvs/is_node_alive",
                        timeout=0.1
                    )
                    # Append to both lists accordingly
                    globals.live_nodes.append(ip_port)
                    globals.viewList.append(ip_port)
                except:
                    # Append to both lists accordingly
                    globals.dead_nodes.append(ip_port)
                    globals.viewList.append(ip_port)
            # Lexicographically sort our all our lists as a sanity check
            globals.viewList = sorted(globals.viewList)
            globals.live_nodes = sorted(globals.live_nodes)
            globals.dead_nodes = sorted(globals.dead_nodes)
        # Return the final flag that determines if there is a problem in our network
        return network_change

    """
        Helper function to intiate read repair amongst all the nodes in our live view list. Initiate the chain of
        requests to be sent to the next node, and so forth. Once you receive the full dictionary, then update your
        values accordingly. This helper method is only to be used if your view of the network is greater than one
        (in other words, if you aren't the only node in your live_nodes view)
        :return void
    """
    def read_repair(self):
        # Now, we will create a temporary list that we will iterate through during read repair. In this fashion,
        # we want all the live nodes, and the caller at the beginning of his partition list, in this temporary
        # list, called iter_list
        iter_list = []
        # This is all the nodes in our partition
        nodes_in_my_partition = self.nodes_in_my_partition()
        # Take out our IP:PORT value from the list
        nodes_in_my_partition.remove(globals.localIPPort)
        # Now, we will bring our IP:PORT value in front of this list
        nodes_in_my_partition.insert(0, globals.localIPPort)
        # Now, we need to insert the nodes in our partition into the "iter_list"
        iter_list.extend(nodes_in_my_partition)
        # Now, we must append all the other values not in our partition, into a new list called "iter_list"
        for partition_id, array_of_nodes in globals.PartitionDict.items():
            # Don't append our replicas again
            if globals.localIPPort in array_of_nodes:
                pass
            # Extend the iter_list to include all the other live nodes
            else:
                iter_list.extend(array_of_nodes)
        # Now we will begin to iterate through the organized "iter_list"
        try:
            if globals.localIPPort == iter_list[0]:
                next_node = iter_list[1]
            else:
                next_node = iter_list[0]
        # For the case of deletes, there will be no next node if there are only two more in the network, so we will return
        # and make the chain-like broadcast code unreachable
        except IndexError:
            return
        # Create a new dictionary to be passed
        passing_dict = dict()
        # List of keys to be deleted from our dictionary
        keys_to_be_deleted = []
        # Instantiate logic for determining which values do not belong to you
        for key in globals.KVSDict:
            # Get the key hash for this key
            key_hash = self.init_hash(key)
            # If the hash is still in this range, then don't do anything.
            if key_hash in self.my_range():
                pass
            # If the hash is no longer in this range, then delete the key and send a request to the right node to
            # write it to their local KVS
            else:
                passing_dict[key] = globals.KVSDict[key]
                # Append to the list of keys to be deleted from our KVS
                keys_to_be_deleted.append(key)
        # Iterate through the keys
        for key in keys_to_be_deleted:
            # Quietly delete this key that no longer belongs in your local KVS
            del globals.KVSDict[key]
        # Begin the traversal, sending your KVS down the wire to be processed by all other nodes in your data partition
        final_response = requests.put(
            "http://" + str(next_node) + "/kvs/read_repair",
            data={
                'caller': globals.localIPPort,
                'passing_dict': json.dumps(passing_dict),
                'intra_dict': json.dumps(globals.KVSDict),
                'iter_list': json.dumps(iter_list),
                'live_nodes': json.dumps(globals.live_nodes),
                'dead_nodes': json.dumps(globals.dead_nodes),
                'view_list': json.dumps(globals.viewList)
            }
        )
        # Get the JSON that was passed through *all* the nodes in the network
        metadata = final_response.json()
        logging.debug("Final dictionary passed back to original caller: " + str(metadata))
        # Clear our local KVS and make it equal to the "intra_dict" keys and values
        globals.KVSDict.clear()
        globals.KVSDict.update(metadata['intra_dict'])
        # Extrapolate the metadata from the final callback (we should be expecting a dictionary containing "intra_dict"
        # and "passing_dict"
        final_dict = metadata['passing_dict']
        # Now, we must check vector clock values in the final dictionary passed back to us on the callback
        for key in final_dict:
            # Get the key hash for this key
            key_hash = self.init_hash(key)
            # If the hash is still in this range, then don't do anything.
            if key_hash in self.my_range():
                # If the key exists in the dictionary, then we must compare vector clocks to establish causality and
                # see what we should overwrite
                if key in globals.KVSDict:
                    # Vector clock flag to see which vector clock is greater. If they are concurrent, then we will
                    # compare timestamps
                    vector_clock = self.compare_vector_clocks(
                        globals.KVSDict[key]['vector_clock'], final_dict[key]['vector_clock'])
                    # We check both vector clock value and the timestamp to see if the final dictionary wins,
                    # and if it is the case that it does win for either instance, then we can update this accordingly
                    if final_dict[key]['timestamp'] > globals.KVSDict[key]['timestamp'] or vector_clock is False:
                        globals.KVSDict[key] = final_dict[key]
                # If the key doesn't already exist, simply append it to the final_dict
                else:
                    globals.KVSDict[key] = final_dict[key]

    """
        Simple hash function (the inital one) that will return the appropriate bin that the key should go to.
        :var key: The key for each value in the dictionary.
        :return: Hash value, as an int
    """
    def init_hash(self, key):
        return int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16) % globals.upperBound

    """
        Initialize the ranges and total number of data partitions in the network.
        :param num_of_partitions: The number of partitions to be determined by the caller (divide the length of the
                                    current viewList by the number of replicas per data partition, "K")
        :return void
    """
    def init_ranges(self, num_of_partitions):
        # Clear both dictionaries (if there are any values in them to begin with)
        globals.RangesDict.clear()
        globals.PartitionDict.clear()
        # Initialize the range (0 - [upper bound])
        total_range = range(0, globals.upperBound)
        # Initialize the number of ranges, or bins
        num_of_ranges = int(globals.upperBound / num_of_partitions)
        # Create a range list to be mapped to each partition ID
        range_list = [total_range[x:x + num_of_ranges] for x in range(0, len(total_range), num_of_ranges)]
        # Lexically sort our view list
        # globals.viewList = sorted(globals.viewList)
        # Map each range to their appropriate partition ID
        i = 0
        for range_val in range_list:
            globals.RangesDict[i] = range_val
            i = i + 1
        # Now, we will splice the list of IP:PORT values in the viewList and create a new list of lists, as determined
        # by K (e.g. if we have 4 IP:PORT values and K = 1, then we will have them as such: [[IP], [IP], [IP], [IP]]).
        # With this new list, 'partition_list', we will initialize PartitionDict with partition IDs and their
        # corresponding array of IP:PORT values
        partition_list = [globals.live_nodes[x:x + globals.value_of_k]
                          for x in range(0, len(globals.live_nodes), globals.value_of_k)]
        # Now, iterate through 'partition_list' and append accordingly
        i = 0
        for element in partition_list:
            globals.PartitionDict[i] = element
            i = i + 1

    """
        Helper function that will get the range for this specific node
        :return Python Range object
    """
    def my_range(self):
        for partition_id, array_of_nodes in globals.PartitionDict.items():
            if globals.localIPPort in array_of_nodes:
                return globals.RangesDict[partition_id]

    """
        Helper function that will get our partition ID
        :return int, partition ID
    """
    def my_partition_id(self):
        # Once you find the intended partition ID, then map it to the partition ID here and find the
        # subset of intended nodes
        for partition_id, array_of_nodes in globals.PartitionDict.items():
            if globals.localIPPort in array_of_nodes:
                return partition_id

    """
        Helper function to get the nodes in my partition.
        :return an array of nodes, the ones that belong in my partition
    """
    def nodes_in_my_partition(self):
        for partition_id, array_of_nodes in globals.PartitionDict.items():
            if globals.localIPPort in array_of_nodes:
                return array_of_nodes

    """
        Intuitive function to get the intended IPPORT value, or node, to forward a particular request,
        based on if the key's hash value is in the range.
        :param key_hash: The key_hash value associated with the key given by the client
        :return: ip_port, a string representing the intended node to forward the request to
    """
    def get_intended_nodes(self, key_hash):
        try:
            intended_partition = None
            intended_nodes = None
            # Iterate through both dictionaries, and find an appropriate node to send the value to
            for partition_id, range in globals.RangesDict.items():
                if key_hash in range:
                    intended_partition = partition_id
                    break
            # Once you find the intended partition ID, then map it to the partition ID here and find the
            # subset of intended nodes
            for partition_id, array_of_nodes in globals.PartitionDict.items():
                if partition_id == intended_partition:
                    intended_nodes = array_of_nodes
                    break
            # Basic error handling
            if intended_nodes is None:
                raise Exception('No value found when searching for a node to write a value to.')
            # Send to all nodes in the data partition
            return intended_nodes
        except Exception as e:
            logging.debug(e)

    """
        Helper function to construct a vector clock for any given value written to the KVS.
        :return: Built string in the following form, <0.0.0.0.0> 
    """
    def construct_vector_clock(self):
        # Append all vector clock values to this mutable array, to be all joined after
        vc_string_builder = []
        # Iterate for all IP:PORT values in our view list, whether they are dead or alive
        for ip_port in globals.viewList:
            vc_string_builder.append(str(globals.VCDict[ip_port]))
        # Return the concatenated string to the caller, to return to the client
        return "<" + ".".join(vc_string_builder) + ">"

    """
        Helper function to merge the incoming vector clock, as defined by the client, and our vector clock
        values. A precondition to this is that the list will always be the same length as our dictionary.
        If for any reason one is longer than the other, then they will be concurrent.
        :param client_vc: The vector clock, as defined by the user as a string, to be mapped to our VCDict
                            where appropriate.
        :return void
    """
    def map_client_vc(self, client_vc):
        try:
            # Deconstruction of the client_vc from a string to an iterable
            client_vc = iter(client_vc.replace("<", "").replace(">", "").split("."))
            # Map the client_vc values to our VCDict accordingly
            for ip_port in globals.viewList:
                globals.VCDict[ip_port] = next(client_vc)
        except Exception as e:
            logging.error(e)

    """
        Helper function to map the client's vector clock values to ours, in the case that both are concurrent,
        so this is a precondition. They are not of equal length, so we must take this into account.
        :param client_vc: The vector clock, as defined by the user as a string, to be mapped to our VCDict
                    where appropriate.
        :return void
    """
    def map_client_vc_concurrent(self, client_vc):
        # Deconstruction of the client_vc from a string to an iterable
        client_vc = iter(client_vc.replace("<", "").replace(">", "").split("."))
        # Map the client_vc values to our VCDict, where applicable. They are not of equal length, so it is
        # possible that it will throw an exception. In the case that it does, we simply break and leave
        # the rest of the values alone.
        for ip_port in globals.viewList:
            try:
                their_value = next(client_vc)
                our_value = globals.VCDict[ip_port]
                # Make the value in our VCDict equal to the max between the two values
                globals.VCDict[ip_port] = max(their_value, our_value)
            # Break if the iterable is no longer iterable, or if there is an ip_port value in our dictionary
            # that no longer exists
            except:
                break

    """
        Helper function to compare vector clock values. If they are equal, then compare time stamps.
        :param clock1: The first clock passed in the function (String)
        :param clock2: The second clock passed in the function (String)
        :return: True, if the first clock wins, False, if the second clock wins, and None if they are
                    deemed concurrent
    """
    def compare_vector_clocks(self, first_clock, second_clock):
        # Split the clock values accordingly
        first_clock = first_clock.replace("<", "").replace(">", "").split(".")
        second_clock = second_clock.replace("<", "").replace(">", "").split(".")
        # If one is greater than the other, then they are concurrent, and we can return None
        if len(first_clock) != len(second_clock):
            return None
        # Flags to determine which vector clock is greater
        first_flag = False
        second_flag = False
        # Go through the iterations and compare accordingly
        for first, second in zip(first_clock, second_clock):
            # Changing the flags based on the vector clock values
            if int(first) > int(second):
                first_flag = True
            elif int(second) > int(first):
                second_flag = True
            else:
                pass
            # Return immediately if a decision can be made on the winner
            if first_flag is True and second_flag is False:
                return True
            elif first_flag is False and second_flag is True:
                return False
            else:
                return None

    """
        Helper function to increment all vector clocks for a given IP:PORT value to ALL nodes. Any unreachable nodes
        will simply be ignored. This is done during writes.
        :param ip_port: Given ip_port value to be incremented
        :return void
    """
    def increment_vector_clock(self, ip_port):
        # Increment your own vector clock value
        try:
            globals.VCDict[ip_port] = globals.VCDict[ip_port] + 1
        # For whatever reason this vector clock value doesn't exist in your VCDict, then add it accordingly
        except:
            globals.VCDict[ip_port] = 1
        # Now iterate through all nodes, dead and alive, and try to increment your vector clock value on as many
        # nodes as you can
        for node in globals.viewList:
            # Don't ping yourself
            if node == globals.localIPPort:
                continue
            # Try to reach that node, and if you cannot, then simply pass
            try:
                # Forward the request
                requests.put(
                    "http://" + str(node) + "/kvs/increment_vector_clock",
                    data={'ip_port': ip_port},
                    timeout=0.1
                )
            except:
                pass
