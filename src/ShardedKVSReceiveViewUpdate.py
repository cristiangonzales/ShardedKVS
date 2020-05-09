"""
    Author: Cristian Gonzales
"""

from flask import Response
from flask import request
from flask.json import jsonify
from flask_restful import Resource

import json
import globals

"""
    "Helper endpoint" for helping add and delete nodes. A PUT request indicates an add, and storing information, and a
    GET request indicates the deletion of a node, in which case you would send your KVS back.
"""
class ShardedKVSReceiveViewUpdate(Resource):

    """
        Deleting a node. We request their dictionary to send down the pipeline and redistribute values accordingly.
        :return HTTP response
    """
    def get(self):
        return jsonify(globals.KVSDict)

    """
        Adding a node. We will take the value "K" given to us and initialize it appropriately for the added node.
        :return HTTP request
    """
    def put(self):
        # Initialize your view list and the value of K
        globals.viewList = json.loads(request.form['view_list'])
        globals.value_of_k = json.loads(request.form['k'])
        self.init_vector_clock_values()

    """
        For all other nodes in the network other than the one being added or deleted, and the caller, we want to tell 
        them to delete or add the IP:PORT value in their VCDict data.
        structures
    """
    def post(self):
        # Request arguments
        request_type = request.args['type']
        ip_port = request.args['ip_port']
        # In the case that the caller requests an "adding" of a node
        if request_type == "add":
            globals.VCDict[ip_port] = 0
        # The case where the user requests to delete a node
        else:
            del globals.VCDict[ip_port]

    """
        Helper function to initialize all vector clocks to 0, to begin with. Intended for one time use.
        :return void
    """
    def init_vector_clock_values(self):
        for node in globals.viewList:
            globals.VCDict[node] = 0

