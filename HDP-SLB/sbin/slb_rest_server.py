#!/usr/bin/env python

import slb_doc_graph_crud as dgc
import json
from flask import Flask, request, jsonify
from flask.ext.restful import reqparse, abort, Api, Resource

app = Flask(__name__)
api = Api(app)

_client_handle = dgc._client_handle


def abort_if_artifact_doesnt_exist(artifact_name):
    if artifact_name not in ['xxxx']: # to be implemented
        abort(404, message="Artifact {} doesn't exist".format(artifact_name))


class Artifact(Resource):
    def get(self, artifact_name):
        _return_json = dgc._do_action_on_artifact('get', None, artifact_name)
        return jsonify(_return_json)

    def delete(self, artifact_name):
        return {"message": "SLB: Nothing implemented as yet for delete request on this endpoint"}

    def put(self, artifact_name):
        return {"message": "SLB: Nothing implemented as yet for put request on this endpoint"}


class ArtifactList(Resource):
    def get(self):
        return {"message": "SLB: Nothing implemented as yet for get request on this endpoint"}

    def post(self):
        _return_json = dgc._do_action_on_artifact('put', request.json, None)
        return jsonify(_return_json)

    def put(self):
        _return_json = dgc._do_action_on_artifact('put', request.json, None)
        return _return_json


class ArtifactTreeAll(Resource):
    def get(self, artifact_name):
        _return_json = dgc._do_action_on_relation('all', artifact_name)
        return jsonify(_return_json)


class ArtifactTreeChildren(Resource):
    def get(self, artifact_name):
        _return_json = dgc._do_action_on_relation('children', artifact_name)
        return jsonify(_return_json)


class ArtifactTreeParent(Resource):
    def get(self, artifact_name):
        _return_json = dgc._do_action_on_relation('parent', artifact_name)
        return jsonify(_return_json)


# Adding rest api resources
api.add_resource(ArtifactList, '/api/v1/artifact')
api.add_resource(Artifact, '/api/v1/artifact/<string:artifact_name>')
api.add_resource(ArtifactTreeAll, '/api/v1/artifact/tree/all/<string:artifact_name>')
api.add_resource(ArtifactTreeChildren, '/api/v1/artifact/tree/children/<string:artifact_name>')
api.add_resource(ArtifactTreeParent, '/api/v1/artifact/tree/parent/<string:artifact_name>')


if __name__ == '__main__':
    app.run('0.0.0.0')

