"""
    Author: Cristian Gonzales
"""

from flask import Response
from flask import request
from flask_restful import Resource

import json
import globals

"""
    Endpoint to "brute-force" write to a node that is a replica in your data partition.
    :return HTTP response
"""
class ShardedKVSMyReplicaWrite(Resource):
    def put(self):
        key = request.form['key']
        data = json.loads(request.form['data'])
        # Store it!
        globals.KVSDict[key] = data
        # Return the response accordingly
        return Response(
            json.dumps({'msg': 'success', 'success': 'value stored to replica in your partition'}),
            status=200,
            mimetype='application/json'
        )
