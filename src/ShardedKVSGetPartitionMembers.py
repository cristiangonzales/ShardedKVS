"""
    Author: Cristian Gonzales
"""

from flask import Response
from flask import request
from flask_restful import Resource

import logging
import json
import globals
import requests

"""
    Function that will return all the nodes for a requested partition ID, in the network view.
    Services HTTP GET requests only.
    :return HTTP response
"""
class ShardedKVSGetPartitionMembers(Resource):
    def get(self):
        # Request arguments passed in body of HTTP request by the client
        partition_id = int(request.args['partition_id'])
        # Error checking if the client did not pass the appropriate arguments
        if partition_id is None:
            return Response(
                json.dumps({
                    'msg': 'error',
                    'error': 'please pass a \'partition_id\' argument in the HTTP GET request body'
                }),
                status=404,
                mimetype='application/json'
            )
        # Get the temporary "partition_dict" to return a response to the client, ensuring fresh data. This "health
        # check" will determine all the live nodes in our network view, and we will be able to determine a temporary
        # "PartitionDict" while not affecting our global variables. We do this because it will affect our read_repair
        # detecting a network change (this is bad)
        partition_dict = self.lean_health_check()
        # Wrap in a try catch because if the client passed an invalid request argument in 'partition_id', then we obviously
        # cannot index it and it will throw an error
        try:
            # Return the HTTP response to the client
            return Response(
                json.dumps({
                    'msg': 'success',
                    'partition_members': partition_dict[partition_id]
                }),
                status=200,
                mimetype='application/json'
            )
        except:
            return Response(
                json.dumps({
                    'msg': 'error',
                    'error': 'please pass a valid \'partition_id\' argument in the HTTP GET request body'
                }),
                status=404,
                mimetype='application/json'
            )

    """
        This is a "temporary" health check to see which nodes in our view list are dead and alive. We do not want to update
        our lists entirely, but rather, we want to be able to ping all nodes and see what is dead and alive, and create temporary
        lists to pass to another helper function and return a PartitionDict dictionary, without affecting the *actual* PartitionDict.
        This ensures that this endpoint will always be giving fresh data, while not affecting our read repair's ability to detect a 
        network change.
        :return a dictionary that will serve as our "temporary" partition dictionary
    """
    def lean_health_check(self):
        # Flag to determine if we need to redistribute values and ranges
        network_change = False
        # Temporary dead nodes list
        dead_nodes = []
        # Temp list is used to get the set difference, view_list is our temporary "view_list", "live_nodes" is our
        # temporary live nodes list, and they are all equal to the actual view list
        temp_list = []
        live_nodes = []
        temp_list.extend(globals.viewList)
        live_nodes.extend(globals.viewList)
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
                if ip_port in dead_nodes:
                    # Removing a node from the dead nodes list, and appending back to life
                    try:
                        dead_nodes.remove(ip_port)
                    except ValueError:
                        pass
                    live_nodes.append(ip_port)
                    # Change the broadcast flag, detecting a network change
                    network_change = True

            except:
                # A formerly live node is now dead, with the caveat that the live node might already be dead
                # (we add a conditional for this reason)
                if ip_port in live_nodes:
                    try:
                        live_nodes.remove(ip_port)
                    except ValueError:
                        pass
                    # Append the node to the dead list and flip the flag to True
                    dead_nodes.append(ip_port)
                    network_change = True
        # If there is indeed a network change, ping the nodes in the set difference between our temp list and our
        # current view list, and see if they're alive
        if network_change:
            # Lexicographically sort our temporary view_list and eliminate duplicates
            temp_list = list(set((temp_list)))
            # The set difference between the "temp" list and the current view list
            new_nodes = sorted(list(set(temp_list) - set(globals.viewList)))
            # For each IPPORT value in the sorted list of the set difference between our compiled list of nodes and
            # our current view list, and ping them to see if they are alive
            for ip_port in new_nodes:
                try:
                    requests.get(
                        "http://" + str(ip_port) + "/kvs/is_node_alive",
                        timeout=0.1
                    )
                    # Append to the live_nodes list accordingly
                    live_nodes.append(ip_port)
                except:
                    pass
            # Lexicographically sort our all our lists as a sanity check
            live_nodes = sorted(live_nodes)
        # Return the product of "partition_dict" back to the original caller
        return self.partition_dict(live_nodes)

    """
        Forming a temporary partition dictionary to return back to the caller.
        :param live_nodes: The temporary "live_nodes" list, without directly affecting "live_nodes"
        :return partition_dict, a temporary dictionary
    """
    def partition_dict(self, live_nodes):
        # Create a temporary "partition_dict"
        partition_dict = dict()
        # Now, we will splice the list of IP:PORT values in the viewList and create a new list of lists, as determined
        # by K (e.g. if we have 4 IP:PORT values and K = 1, then we will have them as such: [[IP], [IP], [IP], [IP]]).
        # With this new list, 'partition_list', we will initialize PartitionDict with partition IDs and their
        # corresponding array of IP:PORT values
        partition_list = [live_nodes[x:x + globals.value_of_k]
                          for x in range(0, len(live_nodes), globals.value_of_k)]
        # Now, iterate through 'partition_list' and append accordingly
        i = 0
        for element in partition_list:
            partition_dict[i] = element
            i = i + 1
        # Return this "partition_dict" back to the caller
        return partition_dict
