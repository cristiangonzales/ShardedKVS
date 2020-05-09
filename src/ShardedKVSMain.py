"""
    Author: Cristian Gonzales
"""

import logging

from flask import Flask
from flask_restful import Api

import sys; sys.path.append('src/')
import os

from ShardedKVS import ShardedKVS
from ShardedKVSGetPartitionID import ShardedKVSGetPartitionID
from ShardedKVSGetAllPartitionIDs import ShardedKVSGetAllPartitionIDs
from ShardedKVSGetPartitionMembers import ShardedKVSGetPartitionMembers
from ShardedKVSIsNodeAlive import ShardedKVSIsNodeAlive
from ShardedKVSGetNumberOfKeys import ShardedKVSGetNumberOfKeys
from ShardedKVSReadRepair import ShardedKVSReadRepair
from ShardedKVSMyReplicaWrite import ShardedKVSMyReplicaWrite
from ShardedKVSIncrementVectorClock import ShardedKVSIncrementVectorClock
from ShardedKVSDataPartitionWrite import ShardedKVSDataPartitionWrite
from ShardedKVSQuickRead import ShardedKVSQuickRead
from ShardedKVSViewUpdate import ShardedKVSViewUpdate
from ShardedKVSReceiveViewUpdate import ShardedKVSReceiveViewUpdate

import globals
from math import ceil
from gevent.wsgi import WSGIServer

"""
    Main callable class file for Homework 4: A Fault-Tolerant & Scalable Key-Value Store
"""
class ShardedKVSMain:

    def __init__(self):
        # Initialize the application with the appropriate route
        app = Flask(__name__)
        api = Api(app)

        # Environment variables
        globals.localIPPort = os.getenv('ip_port')
        logging.debug("Value of IP:PORT: " + str(globals.localIPPort))

        # The initial VIEW list
        if os.getenv('VIEW') is not None:
            # Number of nodes per partition
            globals.value_of_k = int(os.getenv('K'))
            # View list and the number of nodes in the view list
            logging.debug("Number of replicas per partition: " + str(globals.value_of_k))
            globals.viewList = os.getenv('VIEW').split(",")
            logging.debug("List of all IP:PORT values in the VIEW: " + str(globals.viewList))
            logging.debug("Number of nodes: " + str(len(globals.viewList)))
            # Initialize all vector clock values to be 0 to begin with
            self.init_vector_clock_values()
            # Make our live_nodes list equal to our viewList initially
            globals.live_nodes = sorted(globals.viewList)
            # Initialize the number of current partitions in our network
            # We find this by dividing the number of nodes in the entire network by the number of replicas
            # per data partition. This will give us the total number of partitions, or "bins"
            num_of_partitions = ceil(len(globals.live_nodes) / globals.value_of_k)
            # Initialize the initial amount of ranges and IDs
            self.init_ranges(num_of_partitions)

        # Add the appropriate endpoints
        api.add_resource(ShardedKVS, '/kvs')
        api.add_resource(ShardedKVSGetPartitionID, '/kvs/get_partition_id')
        api.add_resource(ShardedKVSGetAllPartitionIDs, '/kvs/get_all_partition_ids')
        api.add_resource(ShardedKVSGetPartitionMembers, '/kvs/get_partition_members')
        api.add_resource(ShardedKVSIsNodeAlive, '/kvs/is_node_alive')
        api.add_resource(ShardedKVSGetNumberOfKeys, '/kvs/get_number_of_keys')
        api.add_resource(ShardedKVSReadRepair, '/kvs/read_repair')
        api.add_resource(ShardedKVSMyReplicaWrite, '/kvs/my_replica_write')
        api.add_resource(ShardedKVSIncrementVectorClock, '/kvs/increment_vector_clock')
        api.add_resource(ShardedKVSDataPartitionWrite, '/kvs/data_partition_write')
        api.add_resource(ShardedKVSQuickRead, '/kvs/quick_read')
        api.add_resource(ShardedKVSViewUpdate, '/kvs/view_update')
        api.add_resource(ShardedKVSReceiveViewUpdate, '/kvs/receive_view_update')

        # Run the host
        local_port = int(globals.localIPPort.split(":")[1])
        logging.info(" * Starting server " + str(globals.localIPPort))
        WSGIServer(('0.0.0.0', local_port), app).serve_forever()
        # app.run(host='0.0.0.0', port=int(local_port))

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
        globals.viewList = sorted(globals.viewList)
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
        Helper function to initialize all vector clocks to 0, to begin with. Intended for one time use.
        :return void
    """
    def init_vector_clock_values(self):
        for node in globals.viewList:
            globals.VCDict[node] = 0

# Initiate a new thread for the method
if __name__ == '__main__':
    try:
        # Setting root logging level to DEBUG
        logging.basicConfig(level=logging.DEBUG)
        # Instantiate the class objects
        globals.globals()
        ShardedKVSMain()
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logging.error(str(e))
