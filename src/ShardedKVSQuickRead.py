"""
    Author: Cristian Gonzales
"""

from flask import Response
from flask import request
from flask_restful import Resource

import json
import globals

"""
    Endpoint to do a "quick read" from a given node. Because our read repair ensures replication, we can trust that each
    node will have the same information. So, if the key does not exist, we do not need to check our replicas. We can simply
    assume that the requested key does not exist.
    :return HTTP response
"""
class ShardedKVSQuickRead(Resource):
    def get(self):
        try:
            # Get the requested key from the HTTP body
            key = request.args['key']
            # Return the appropriate response
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
        # In the case that the key does not exist, return the appropriate HTTP response
        except:
            return Response(
                json.dumps({
                    'msg': 'error',
                    'error': 'key does not exist in the kvs'
                }),
                status=404,
                mimetype='application/json'
            )

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
