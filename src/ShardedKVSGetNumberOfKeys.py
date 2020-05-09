"""
    Author: Cristian Gonzales
"""

from flask import Response
from flask_restful import Resource

import json
import globals

"""
    Function that will serve as an endpoint to return the number of keys in this instance's KVS, to the client.
    :return HTTP responses (varying)
"""
class ShardedKVSGetNumberOfKeys(Resource):

    def get(self):
        return Response(
            json.dumps({'count': len(globals.KVSDict)}),
            status=200,
            mimetype='application/json'
        )
