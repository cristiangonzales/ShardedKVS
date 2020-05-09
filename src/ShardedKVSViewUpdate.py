"""
    Author: Cristian Gonzales
"""

import logging

from flask import Response
from flask import request
from flask_restful import Resource

import json
from math import ceil
import hashlib
import requests
import globals

"""
    
"""
class ShardedKVSViewUpdate(Resource):
    """
        PUT request instantiated by the client.
        :return HTTP request
    """
    def put(self):
        try:
            # Requested IPPORT value to be removed or added  by the client
            ip_port = request.form['ip_port']
            # The case that the client requests to add a node
            if request.form['type'] == "add":
                # If the requested ip_port already exists, return an error HTTP response
                if ip_port in globals.viewList:
                    return Response(
                        json.dumps({'msg': 'error', 'error': 'requested add already exists in the network'}),
                        status=404,
                        mimetype='application/json'
                    )
                # Append the value to your view list
                globals.viewList.append(ip_port)
                globals.live_nodes.append(ip_port)
                # Perform the health check across the network (we do not need to ping the newly added node, so we are
                # assuming that the added node is going to be alive to begin with)
                self.node_health_check(ip_port)
                # If there is a network change, reinitialize your own ranges by calculating the new number of partitions
                num_of_partitions = ceil(len(globals.live_nodes) / globals.value_of_k)
                # Initialize the initial amount of ranges and IDs
                self.init_ranges(num_of_partitions)
                # Now, tell all other nodes to initialize the IP_PORT value in their dictionaries (other than yourself
                # and the node being added)
                for node in globals.live_nodes:
                    # Don't ping yourself or the node being added to the network
                    if node == globals.localIPPort or node == ip_port:
                        continue
                    requests.post(
                        "http://" + node + "/kvs/receive_view_update?type=add&ip_port=" + ip_port,
                        timeout=0.1
                    )
                # Now, we will send a request to the newly added live node to initialize their view list and value of K
                requests.put(
                    "http://" + ip_port + "/kvs/receive_view_update",
                    data={'view_list': json.dumps(globals.viewList), 'k': globals.value_of_k},
                    timeout=0.1
                )
                # After updating everything, we will instantiate the chain of updating all dictionaries (read repair)
                self.read_repair()
                # After read repair has finished, then we can return the appropriate response to the client
                return Response(
                    json.dumps({
                        "msg": "success",
                        "partition_id": self.my_partition_id(),
                        "number_of_partitions": num_of_partitions
                    }),
                    status=201,
                    mimetype='application/json'
                )
            # The case that the client requests to remove a node
            elif request.form['type'] == "remove":
                # If the requested node is not in our views list, it does not exist, and we need to tell the client appropriately
                if ip_port not in globals.viewList:
                    return Response(
                        json.dumps({'msg': 'error', 'error': 'requested node to be deleted does not exists in the network'}),
                        status=404,
                        mimetype='application/json'
                    )
                # Remove this value from the global viewList
                globals.viewList.remove(ip_port)
                # Return error if they delete all nodes in the network
                if len(globals.viewList) == 0:
                    return Response(
                        json.dumps({'msg': 'error', 'error': 'at least one node must exist in the network'}),
                        status=404,
                        mimetype='application/json'
                    )
                # Try-catch (as the value we are removing might be in either list)
                try:
                    globals.live_nodes.remove(ip_port)
                except:
                    try:
                        globals.dead_nodes.remove(ip_port)
                    except:
                        pass
                # Perform the health check across the network (we do not need to ping the newly deleted node, so we are
                # assuming that the added node is going to be alive to begin with)
                self.node_health_check(ip_port)
                # If there is a network change, reinitialize your own ranges by calculating the new number of partitions
                num_of_partitions = ceil(len(globals.live_nodes) / globals.value_of_k)
                # Initialize the initial amount of ranges and IDs
                self.init_ranges(num_of_partitions)
                # Now, tell all other nodes to initialize the IP_PORT value in their dictionaries (other than yourself
                # and the node being added)
                for node in globals.live_nodes:
                    # Don't ping yourself or the node being added to the network
                    if node == globals.localIPPort or node == ip_port:
                        continue
                    requests.post(
                        "http://" + node + "/kvs/receive_view_update?type=remove&ip_port=" + ip_port,
                        timeout=0.1
                    )
                # Now, we check if we're the node being deleted. If this is the case, then we need to pass in our dictionary
                # as the values that will be redistributed amongst all the nodes. If this is not the case, then we need to pass
                # in the dictionary of the node that is being deleted
                if globals.localIPPort == ip_port:
                    self.read_repair_del(globals.KVSDict, True)
                # The case where another node is being deleted
                else:
                    # Get the dictionary of this node to be deleted
                    resp = requests.get(
                        "http://" + ip_port + "/kvs/receive_view_update",
                        timeout=0.1
                    )
                    self.read_repair_del(resp.json(), False)
                # Return a response to the client
                return Response(
                    json.dumps({'msg': 'success', "number_of_partitions": num_of_partitions}),
                    status=200,
                    mimetype='application/json'
                )
            # Invalid argument made by the client
            else:
                raise Exception('Invalid request made by the client')
        except Exception as e:
            return Response(
                json.dumps({'msg': 'error', 'error': str(e)}),
                status=404,
                mimetype='application/json'
            )

    """
        Helper function to check which nodes in the network are dead to your view, and which are alive. In the case that
        there is a change, a flag will be returned to the caller and this will indicate that there was indeed a change
        in this node's view of the network. We will also get everyone's view list, append their values to ours, eliminate
        duplicates, and lexigraphically sort it (this is in the case of a network heal and they have added nodes that
        we do not know about).
        :param requested_node: The ip_port value being added or deleted from the network (we do not need to ping them)
        :return void
    """
    def node_health_check(self, requested_node):
        # Flag to determine if we need to redistribute values and ranges
        network_change = False
        # List that is equal to our current view list, but we will append new nodes accordingly
        temp_list = []
        temp_list.extend(globals.viewList)
        # Iterate through all IP:PORT values in the view list and ping them accordingly, in a for loop
        for ip_port in globals.viewList:
            # Don't ping yourself, and continue onto the next iteration
            if ip_port == globals.localIPPort or ip_port == requested_node:
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
                    if vector_clock is False:
                        globals.KVSDict[key] = final_dict[key]
                    if final_dict[key]['timestamp'] > globals.KVSDict[key]['timestamp']:
                        globals.KVSDict[key] = final_dict[key]
                # If the key doesn't already exist, simply append it to the final_dict
                else:
                    globals.KVSDict[key] = final_dict[key]

    """
        Helper function to intiate read repair amongst all the nodes in our live view list. Initiate the chain of
        requests to be sent to the next node, and so forth. Once you receive the full dictionary, then update your
        values accordingly. This helper method is only to be used if your view of the network is greater than one
        (in other words, if you aren't the only node in your live_nodes view)
        :param dict_of_values: The values that we will be passed to the 'read_repair' endpoint
        :param delete_this_instance: Boolean flag to determine if we are deleting ourselves
        :return void
    """
    def read_repair_del(self, dict_of_values, delete_this_instance):
        # Now, we will create a temporary list that we will iterate through during read repair. In this fashion,
        # we want all the live nodes, and the caller at the beginning of his partition list, in this temporary
        # list, called iter_list
        iter_list = []
        # If this instance is not being deleted, then go ahead and put yourself in the front to iterate across this
        # "iter_list". If this instance is being deleted, then simply make your iterable list equal to your current
        # live nodes list
        if not delete_this_instance:
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
        # The case where this instance *is* being deleted
        else:
            iter_list.extend(globals.live_nodes)
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

        # If we are not deleting ourselves, then we will perform the normal computations. If we do delete ourselves,
        # we will pass different parameters in the HTTP body and not process anything on the return
        if not delete_this_instance:
            # Create a new dictionary to be passed
            passing_dict = dict()
            # Now, we append all values from dict_of_values (the KVS of the node being deleted) to our "passing dict"
            passing_dict.update(dict_of_values)
            # List of keys to be deleted from our dictionary
            keys_to_be_deleted = []
            # Instantiate logic for determining which values do not belong to you
            for key in globals.KVSDict:
                # Get the key hash for this key
                key_hash = self.init_hash(key)
                # If the hash is still in this range, then don't do anything.
                if key_hash in self.my_range():
                    pass
                # The key is in the passing_dict, so we have to compare vector clocks
                elif key in passing_dict:
                    vector_clock = self.compare_vector_clocks(
                        globals.KVSDict[key]['vector_clock'], passing_dict[key]['vector_clock'])
                    # Our local KVS key wins, so we write it in the passing_dict accordingly
                    if vector_clock is True or globals.KVSDict[key]['timestamp'] > passing_dict[key]['timestamp']:
                        passing_dict[key] = globals.KVSDict[key]
                    else:
                        pass # TODO
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
                        if vector_clock is False:
                            globals.KVSDict[key] = final_dict[key]
                        if final_dict[key]['timestamp'] > globals.KVSDict[key]['timestamp']:
                            globals.KVSDict[key] = final_dict[key]
                    # If the key doesn't already exist, simply append it to the final_dict
                    else:
                        globals.KVSDict[key] = final_dict[key]
        # The case where we are deleting ourselves (let all other nodes do processing and we do not need to worry about
        # our instance
        else:
            requests.put(
                "http://" + str(next_node) + "/kvs/read_repair",
                data={
                    'caller': globals.localIPPort,
                    'passing_dict': json.dumps(dict_of_values),
                    'intra_dict': json.dumps({}),
                    'iter_list': json.dumps(iter_list),
                    'live_nodes': json.dumps(globals.live_nodes),
                    'dead_nodes': json.dumps(globals.dead_nodes),
                    'view_list': json.dumps(globals.viewList)
                }
            )

    """
        Simple hash function (the inital one) that will return the appropriate bin that the key should go to.
        :var key: The key for each value in the dictionary.
        :return: Hash value, as an int
    """
    def init_hash(self, key):
        return int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16) % globals.upperBound

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
        Helper function to get the nodes in my partition.
        :return an array of nodes, the ones that belong in my partition
    """
    def nodes_in_my_partition(self):
        for partition_id, array_of_nodes in globals.PartitionDict.items():
            if globals.localIPPort in array_of_nodes:
                return array_of_nodes

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
        Helper function that will get the range for this specific node
        :return Python Range object
    """
    def my_range(self):
        for partition_id, array_of_nodes in globals.PartitionDict.items():
            if globals.localIPPort in array_of_nodes:
                return globals.RangesDict[partition_id]
