"""
    Author: Cristian Gonzales
"""

from flask import Response
from flask import request
from flask_restful import Resource

import logging
import time
import json
import requests
import globals

"""
    Endpoint to "brute-force" write to a node that is a replica in your data partition.
    :return HTTP response
"""
class ShardedKVSDataPartitionWrite(Resource):
    def put(self):
        # Data sent by the caller (node from another partition) over the pipeline
        key = request.form['key']
        value = request.form['value']
        caller = request.form['caller']
        # Try to get the causal payload, if it is in your data partition
        try:
            causal_payload = request.form['payload']
        except:
            causal_payload = None

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
        self.increment_vector_clock(globals.localIPPort, caller)
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
        first_flag = second_flag = False
        # Go through the iterations and compare accordingly
        for first, second in zip(first_clock, second_clock):
            if int(first) > int(second):
                first_flag = True
            elif int(second) > int(first):
                second_flag = True
            else:
                pass
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
        :param caller: The caller's IP (such that we don't try to ping it to increment us since it is busy
                        sending a request to us)
        :return void
    """
    def increment_vector_clock(self, ip_port, caller):
        # Increment your own vector clock value
        try:
            globals.VCDict[ip_port] = globals.VCDict[ip_port] + 1
        # For any reason we don't exist in our own dictionary, create an index in our dictionary for ourselves
        except:
            globals.VCDict[ip_port] = 1
        # Now iterate through all nodes, dead and alive, and try to increment your vector clock value on as many
        # nodes as you can
        for node in globals.viewList:
            # Don't ping yourself
            if node == globals.localIPPort or node == caller:
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
