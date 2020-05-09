"""
    Author: Cristian Gonzales
"""

from flask import Response
from flask import request
from flask_restful import Resource

import json
import globals

"""
    Function that will increment the vector clock value for any given IP:PORT value
    :return HTTP response
"""
class ShardedKVSIncrementVectorClock(Resource):
    def put(self):
        # Request arguments passed in body of HTTP request by the client
        ip_port = request.form['ip_port']
        # Try to increment the value of the vector clock for the given IP:PORT, in our VCDict
        try:
            globals.VCDict[ip_port] = globals.VCDict[ip_port] + 1
        # If the value doesn't already exist, then initialize it in our VCDict to be 1 (first time it has been
        # incremented for our dictionary)
        except:
            globals.VCDict[ip_port] = 1
        # Return the HTTP response to the client
        return Response(
            json.dumps({
                'msg': 'success',
                'success': "vector clock for " + str(ip_port) + " incremented"
            }),
            status=200,
            mimetype='application/json'
        )
