"""
    Author: Cristian Gonzales
"""

from flask import Response
from flask_restful import Resource

import json
import globals

"""
    Function that will serve as an endpoint for pinging. If the node is alive (checked through GET), return a 200 OK
    JSON response with their associated view list.
    To initiate the chain of live nodes to be appended, this endpoint will service PUT requests to take the data given
    by the caller, and append their own IP:PORT value to this working list, and pass it on.
    :return HTTP responses (varying)
"""
class ShardedKVSIsNodeAlive(Resource):

    def get(self):
        return Response(
            json.dumps({'msg': 'success', 'success': globals.viewList}),
            status=200,
            mimetype='application/json'
        )
