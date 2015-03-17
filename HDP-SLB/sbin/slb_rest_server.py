#!/usr/bin/env python

import doc_graph_crud
from flask import Flask
from flask.ext.restful import reqparse, abort, Api, Resource

app = Flask(__name__)
api = Api(app)

ARTIFACTS = {
    'artifact1': {'well': 'xxxxxx'},
    'artifact2': {'well': '?????'},
    'artifact3': {'well': 'xxxxx'},
}

def abort_if_artifact_doesnt_exist(artifact_id):
    if artifact_id not in ARTIFACTS:
        abort(404, message="Artifact {} doesn't exist".format(artifact_id))

parser = reqparse.RequestParser()
parser.add_argument('well', type=str)

class Artifact(Resource):
    def get(self, artifact_id):
        abort_if_artifact_doesnt_exist(artifact_id)
        return ARTIFACTS[artifact_id]

    def delete(self, artifact_id):
        abort_if_artifact_doesnt_exist(artifact_id)
        del ARTIFACTS[artifact_id]
        return '', 204

    def put(self, artifact_id):
        args = parser.parse_args()
        welltask = {'well': args['well']}
        ARTIFACTS[artifact_id] = welltask
        return task, 201


class ArtifactList(Resource):
    def get(self):
        return ARTIFACTS

    def post(self):
        args = parser.parse_args()
        artifact_id = 'artifact%d' % (len(ARTIFACTS) + 1)
        ARTIFACTS[artifact_id] = {'well': args['well']}
        return ARTIFACTS[artifact_id], 201


# Adding rest api resources
api.add_resource(ArtifactList, '/artifacts')
api.add_resource(Artifact, '/artifacts/<string:artifact_id>')


if __name__ == '__main__':
    app.run('0.0.0.0')

