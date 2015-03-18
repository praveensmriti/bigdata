#!/usr/bin/env python

import doc_graph_crud as dgc
import json, requests
from flask import Flask, request, jsonify
from flask.ext.restful import reqparse, abort, Api, Resource

app = Flask(__name__)
api = Api(app)

_client_handle = dgc._client_handle

ARTIFACTS = {"artifact1" : {"name" : "well1"},
             "artifact2" : {"name" : "well2"}}

def abort_if_artifact_doesnt_exist(artifact_id):
    if artifact_id not in ARTIFACTS:
        abort(404, message="Artifact {} doesn't exist".format(artifact_id))


class Artifact(Resource):
    def get(self, artifact_id):
        abort_if_artifact_doesnt_exist(artifact_id)
        #return ARTIFACTS[artifact_id]
        return json.dumps(request.json)

    def delete(self, artifact_id):
        abort_if_artifact_doesnt_exist(artifact_id)
        del ARTIFACTS[artifact_id]
        return '', 204

    def put(self, artifact_id):
        #task1 = { "a" : request.data['name']}
        return jsonify(request.json)


class ArtifactList(Resource):
    def get(self):
        return ARTIFACTS

    def post(self):
        args = parser.parse_args()
        artifact_id = 'artifact%d' % (len(ARTIFACTS) + 1)
        ARTIFACTS[artifact_id] = {'well': args['well']}
        return ARTIFACTS[artifact_id], 201

    def put(self):
        args = parser.parse_args()
        artifact_id = 'artifact%d' % (len(ARTIFACTS) + 1)
        ARTIFACTS[artifact_id] = {'well': args['well']}
        return ARTIFACTS[artifact_id], 201


# Adding rest api resources
api.add_resource(ArtifactList, '/api/v1/artifacts')
api.add_resource(Artifact, '/api/v1/artifacts/<string:artifact_id>')


if __name__ == '__main__':
    app.run('0.0.0.0')

