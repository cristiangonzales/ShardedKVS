"""
    Author: Cristian Gonzales
"""

import logging

from flask import request
from flask_restful import Resource
from flask.json import jsonify

import hashlib
from math import ceil
import requests
import json
import globals

"""
    Class that receives any chain request for the background scheduler, and forwards it appropriately. It will return
    when the last node has been reached, and each node will give and take to a working dictionary that is being passed
    through the network.
"""
class ShardedKVSReadRepair(Resource):

    def put(self):
        # Deserialize the JSON objects
        caller = request.form['caller']
        passing_dict = json.loads(request.form['passing_dict'])
        # The intra_dict will not be forwarded when we are at the beginning of a new data partition, so we will simply
        # make it an empty dictionary in this instance. In the case that it is an empty dictionary, we know at the
        # beginning of a data partition.
        try:
            intra_dict = json.loads(request.form['intra_dict'])
        # Make the intra_dict an empty dictionary
        except:
            intra_dict = {}
        iter_list = json.loads(request.form['iter_list'])
        globals.viewList = json.loads(request.form['view_list'])
        globals.live_nodes = json.loads(request.form['live_nodes'])
        globals.dead_nodes = json.loads(request.form['dead_nodes'])
        # If there is a network change, reinitialize your own ranges by calculating the new number of partitions
        num_of_partitions = ceil(len(globals.live_nodes) / globals.value_of_k)
        # Initialize the initial amount of ranges and IDs
        self.init_ranges(num_of_partitions)
        # Fix the keys in both the "intra_dict" and "passing_dict", and initialize those variables to point at them
        # in the case that the caller from the last node passed "intra_dict" in the HTTP request
        metadata = self.fix_both_dict_forward(passing_dict, intra_dict)
        # Extrapolate the passing dictionary and the intrapartition dictionary from the result of the helper method
        passing_dict = metadata['passing_dict']
        intra_dict = metadata['intra_dict']
        # Get the index of our node
        index_of_our_node = iter_list.index(globals.localIPPort)
        # Performing a try-except, if the next node that we choose gives us an IndexError, this
        # indicates that it is nonexistent and we can return a HTTP response to the caller, indicating
        # that all the nodes have updated all their information and we have fallen off the list
        try:
            next_node = iter_list[index_of_our_node + 1]
        except IndexError:
            # Regardless of what data we need to return, we should always make the final "intra_dict" equal to our local
            # KVS (as this will be the final KVS for this data partition)
            globals.KVSDict.clear()
            globals.KVSDict.update(intra_dict)
            # On the IndexError, we have to return back to the original caller. In the case that our index mod with the
            # value of K, or the amount of nodes per data partition, is 0, then we must not return an 'intra_dict'
            # value back to the caller. If not, then we are somewhere at the top of a data partition, so we must not
            # return an 'intra_dict' value
            if index_of_our_node % globals.value_of_k == 0:
                return jsonify({'intra_dict': {}, 'passing_dict': passing_dict})
            # This is the case where we are not at the top of our data partition, so we may return both the intra_dict
            # and the passing_dict
            else:
                return jsonify({'intra_dict': intra_dict, 'passing_dict': passing_dict})
        # In the case that the index of the next node, mod with the value of K (amount of nodes per data partition) is
        # 0, then we must set our KVS to the value of "intra_dict" and forward the request
        if (index_of_our_node + 1) % globals.value_of_k == 0:
            # Clear the values in our KVS, and store the value of "intra_dict" in the local KVS
            globals.KVSDict.clear()
            globals.KVSDict.update(intra_dict)
            # Send our request to the next/first node in the next data partition
            response = requests.put(
                "http://" + next_node + "/kvs/read_repair",
                data={
                    'caller': caller,
                    'passing_dict': json.dumps(passing_dict),
                    'iter_list': json.dumps(iter_list),
                    'live_nodes': json.dumps(globals.live_nodes),
                    'dead_nodes': json.dumps(globals.dead_nodes),
                    'view_list': json.dumps(globals.viewList)
                }
            )
        # In this branch, we will still be passing the "intra_dict" because we have not reached the end of our data
        # partition
        else:
            # Send our request to the next node
            response = requests.put(
                "http://" + next_node + "/kvs/read_repair",
                data={
                    'caller': caller,
                    'passing_dict': json.dumps(passing_dict),
                    'intra_dict': json.dumps(intra_dict),
                    'iter_list': json.dumps(iter_list),
                    'live_nodes': json.dumps(globals.live_nodes),
                    'dead_nodes': json.dumps(globals.dead_nodes),
                    'view_list': json.dumps(globals.viewList)
                }
            )
        # The dictionary, once again, gets sent back to us for more processing (once we get the final response from
        # the last node in the live network)
        returning_data = response.json()
        # Get the data points from the returning data
        final_intra_dict = returning_data['intra_dict']
        final_passing_dict = returning_data['passing_dict']
        # In the case that "final_intra_dict", the returning "intra_dict", is not an empty dictionary, then that means
        # that we make our local KVS equal to this "final_intra_dict". In the case that it is an empty dictionary, then
        # we want to make "final_intra_dict" equal to the final dictionary we have stored in our KVS at the last value.
        # After doing all of this, we shall do work once again witht he "final_passing_dict" and then we can return to
        # our caller
        if final_intra_dict:
            # Store the value of "final_intra_dict" to our local KVS
            globals.KVSDict.clear()
            globals.KVSDict.update(final_intra_dict)
            # Do work to the passing_dictionary, after your KVS has been updated
            final_passing_dict = self.fix_passing_dict(final_passing_dict)
        # This is the case where the first node in the last data partition is returning an empty dictionary to us,
        # so we simply make the "final_intra_dict" equal to our KVS after doing work to it
        else:
            final_passing_dict = self.fix_passing_dict(final_passing_dict)
            final_intra_dict.update(globals.KVSDict)

        # Now, depending on what node we are on, we will return either an empty dictionary, or the value of our KVS
        # back to the caller in "final_intra_dict"
        if index_of_our_node % globals.value_of_k == 0:
            return jsonify({'intra_dict': {}, 'passing_dict': final_passing_dict})
        # This is the case where we are not at the top of our data partition, so we may return both the intra_dict
        # and the passing_dict
        else:
            return jsonify({'intra_dict': final_intra_dict, 'passing_dict': final_passing_dict})

    """
        Reinitialize your keys for the passing_dict and the "intra-partition" dictionary. Here, we are either in the middle
        of our data partition or at the last node of our data partition. This helper method is *only* to be used if we are going
        forward in the "chaining", never on the callback.
        :param passing_dict: The dictionary to have work done to it, passed by the caller.
        :param intra_dict: The dictionary passed by the caller that is a cumulation of all the keys that need to be
                            in each replica. At the end of each data partition, we will store this into memory, so
                            when the callback happens, we can make everyone's KVS equal to this final KVS.
        :return dictionary of new passing_dict and intra_dict to be forwarded if we are still moving forward in our data partition.
    """
    def fix_both_dict_forward(self, passing_dict, intra_dict):
        # Reinitialize this instance's local KVS based on an add or delete node, and if the current key is in the hash.
        # If the key is in this instance's KVS, then delete/keep it appropriately.
        for key in globals.KVSDict:
            # Get the key hash for this key
            key_hash = self.init_hash(key)
            # If the hash is still in this range, then we must check the IntrapartitionDict to see if we need to make
            # changes to it.
            if key_hash in self.my_range():
                # If the key in our KVS isn't in the "intra-dict", then we must add it. If it is in the "intra_dict",
                # then we must determine the most up-to-date value by vector clocks, and then timestamps.
                if key in intra_dict:
                    # Vector clock flag to see which vector clock is greater. If they are concurrent, then we will
                    # compare timestamps
                    vector_clock = self.compare_vector_clocks(
                        globals.KVSDict[key]['vector_clock'], intra_dict[key]['vector_clock'])
                    # This instance's KVS value wins, so we'll overwrite it in the "intra_dict" accordingly. If it is false,
                    # we do not to do anything, and if they are concurrent, then we do not need to do anything. If they are
                    # concurrent, then we must see if this instance's timestamp for the associated value is greater than
                    # intra_dict's timestamp. If it is the inverse, we do not need to do anything.
                    if vector_clock is True or globals.KVSDict[key]['timestamp'] > intra_dict[key]['timestamp']:
                        intra_dict[key] = globals.KVSDict[key]
                # This is the case where the key in our local KVS is not in the "intra" dict
                else:
                    intra_dict[key] = globals.KVSDict[key]
            # If the hash is no longer in this range, then delete the key and send a request to the right node to
            # write it to their local KVS. If the key exists in the passing dictionary, then we must compare vector
            # clocks to establish causality and see what we should overwrite
            elif key in passing_dict:
                # Vector clock flag to see which vector clock is greater. If they are concurrent, then we will
                # compare timestamps
                vector_clock = self.compare_vector_clocks(
                    globals.KVSDict[key]['vector_clock'], passing_dict[key]['vector_clock'])
                # Our local KVS key wins, so we write it in the passing_dict accordingly
                if vector_clock is True or globals.KVSDict[key]['timestamp'] > passing_dict[key]['timestamp']:
                    passing_dict[key] = globals.KVSDict[key]
            # If the key doesn't already exist in the passing_dict, simply append it to the passing_dict
            else:
                passing_dict[key] = globals.KVSDict[key]
        # Create a dictionary for the keys to be appended to the passing_dict (they cannot be added dynamically
        # during iteration)
        keys_for_passing_dict = dict()
        # Now, iterate through all the keys in the dictionary given to you and store them accordingly, if they belong
        # to you
        for key in passing_dict:
            # Get the key hash for this key
            key_hash = self.init_hash(key)
            # If the hash is still in this range, then don't do anything.
            if key_hash in self.my_range():
                # If the key exists in the dictionary, then we must compare vector clocks to establish causality and
                # see what we should overwrite
                if key in intra_dict:
                    # Vector clock flag to see which vector clock is greater. If they are concurrent, then we will
                    # compare timestamps
                    vector_clock = self.compare_vector_clocks(
                        intra_dict[key]['vector_clock'], passing_dict[key]['vector_clock'])
                    # Our local KVS's value wins, so we will store it to the passing dictionary
                    if vector_clock is True or intra_dict[key]['timestamp'] > passing_dict[key]['timestamp']:
                        keys_for_passing_dict[key] = intra_dict[key]
                    # If not, we store it to the working replica "intra_dict" dictionary
                    else:
                        intra_dict[key] = passing_dict[key]
                # If the key doesn't already exist, simply append it to the passing_dict
                else:
                    intra_dict[key] = passing_dict[key]
        # Now, the values added to "keys_for_passing_dict" will be inserted into our "passing_dict"
        for key in keys_for_passing_dict:
            passing_dict[key] = keys_for_passing_dict[key]
        # Return the dictionary of dictionaries back to the caller
        return {'passing_dict': passing_dict, 'intra_dict': intra_dict}

    """
        This method is only to be used if we are at the beginning of a data partition and we wish to fix the "passing_dict"
        only; also, this helper function is only for traversing forward in the network.
        :param passing_dict: The dictionary to have work done to it, passed by the caller.
        :return passing_dict, dictionary
    """
    def fix_passing_dict(self, passing_dict):
        # Initialize a dictionary of keys so that we may append them to our local KVSDict after it has been
        # iterated through (they cannot be added dynamically during iteration)
        keys_for_kvs_dict = dict()
        # Reinitialize this instance's local KVS based on an add or delete node, and if the current key is in the hash.
        # If the key is in this instance's KVS, then delete/keep it appropriately.
        for key in globals.KVSDict:
            # Get the key hash for this key
            key_hash = self.init_hash(key)
            # If the hash is still in this range, then don't do anything.
            if key_hash in self.my_range():
                pass
            # If the key exists in the dictionary, then we must compare vector clocks to establish causality and
            # see what we should overwrite
            elif key in passing_dict:
                # Vector clock flag to see which vector clock is greater. If they are concurrent, then we will
                # compare timestamps
                vector_clock = self.compare_vector_clocks(
                    globals.KVSDict[key]['vector_clock'], passing_dict[key]['vector_clock'])
                # This is the case where our KVS wins, so we overwrite the value in the "passing_dict"
                if vector_clock is True or globals.KVSDict[key]['timestamp'] > passing_dict[key]['timestamp']:
                    passing_dict[key] = globals.KVSDict[key]
                # This is the case where the passing_dict wins, so we will overwrite it in our KVS after the for
                # iteration is over
                else:
                    keys_for_kvs_dict[key] = passing_dict[key]
            # If the key doesn't already exist, simply append it to the passing_dict
            else:
                passing_dict[key] = globals.KVSDict[key]
        # Now, the values added to "keys_for_kvs_dict" will be inserted into our local KVS
        for key in keys_for_kvs_dict:
            globals.KVSDict[key] = keys_for_kvs_dict[key]
        # Create a dictionary for the keys to be appended to the passing_dict (they cannot be added dynamically
        # during iteration)
        keys_for_passing_dict = dict()
        # Now, iterate through all the keys in the dictionary given to you and store them accordingly, if they belong
        # to you
        for key in passing_dict:
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
                        globals.KVSDict[key]['vector_clock'], passing_dict[key]['vector_clock'])
                    # Our local KVS wins, so we store the correct value in the passing dictionary
                    if vector_clock is True or globals.KVSDict[key]['timestamp'] > passing_dict[key]['timestamp']:
                        keys_for_passing_dict[key] = globals.KVSDict[key]
                    # The passing dictionary wins, so we overwrite it in our KVS
                    else:
                        globals.KVSDict[key] = passing_dict[key]
                # If the key doesn't already exist in our KVS and it belongs to us, then store it accordingly
                else:
                    globals.KVSDict[key] = passing_dict[key]
        # Store all the values compiled for the passing dictionary
        for key in keys_for_passing_dict:
            passing_dict[key] = keys_for_passing_dict[key]
        # Return the final dictionary that has been worked on, to the caller
        return passing_dict

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
        Helper function to get the nodes in my partition.
        :return an array of nodes, the ones that belong in my partition
    """
    def nodes_in_my_partition(self):
        for partition_id, array_of_nodes in globals.PartitionDict.items():
            if globals.localIPPort in array_of_nodes:
                return array_of_nodes

    """
        Simple hash function (the inital one) that will return the appropriate bin that the key should go to.
        :param key: The key for each value in the dictionary.
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
