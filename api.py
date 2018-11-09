import sys

from flask import Flask, request
from flask_restful import reqparse, abort, Api, Resource

app = Flask(__name__)
api = Api(app)

# dict to store and list of entries
entries = {}

"""
REST resource for Entries
"""
class Entries(Resource):
    # return length and content of `entries`
    def get(self):
        return {'num_entries': len(entries), 'entries': [entries]}, 200

    # make entry into `entries` with data in body
    def post(self):
        data = request.get_json()
        print("response - {}".format(data))
        for key, value in data.items():
            entries[key] = value
        return None, 201

"""
Api resource routing information
"""
api.add_resource(Entries, '/api/v1/entries')

if __name__ == '__main__':
    app.run(debug=True, port=sys.argv[1])