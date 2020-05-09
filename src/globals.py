"""
    Author: Cristian Gonzales
"""

"""
    Global variables for the entire codebase are initialized here.

    Global variables
    --------------------
    
    Integers
    =============
    :var localIPPort: The localIPPort for this specific instance in the form of IP:PORT (an identifier to query for objects)
                        (*** THIS REMAINS STATIC ***)
    :var value_of_k: The number of replicas "K" per data partition (*** THIS REMAINS STATIC ***)
    :var num_of_partitions: The number of partitions based on the total number of nodes in the network divided by
                            the number of replicas per data partition "K".
    :var job_counter: The job counter
    
    :var upperBound: The upper bound on the maximum number of nodes we suspect will be in our network.
                        In our case, we picked 1,000,000.  (*** THIS REMAINS STATIC ***)
    
    Dictionaries
    ==============
    :var KVSDict: The key-value store, global to all class files
    :var RangesDict: Dictionary that maps "partition IDs" to ranges for the static value chosen
    :var PartitionDict: The dictionary that maps "partition IDs" to an array of IP:PORT values
    :var VCDict: Maps IP:PORT values to VC values

    Lists
    ==============
    :var viewList: The global view of all nodes (IP:PORT values) (*** THIS REMAINS STATIC ***)
    :var live_nodes: The nodes that are "alive" in this instance's view of the network. Initially, this is equal to 
                        the 
    :var dead_nodes: The nodes that are "dead" in this instance's view of the network
"""

class globals:

    # Integer values
    global localIPPort

    global value_of_k

    global upperBound
    upperBound = 1000000

    # Dictionaries
    global KVSDict
    KVSDict = dict()

    global RangesDict
    RangesDict = dict()

    global PartitionDict
    PartitionDict = dict()

    global VCDict
    VCDict = dict()

    # Lists
    global viewList
    viewList = []

    global live_nodes
    live_nodes = []

    global dead_nodes
    dead_nodes = []
