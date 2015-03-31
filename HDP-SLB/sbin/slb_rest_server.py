#!/usr/bin/env python

import slb_doc_graph_crud as dgc
import json
from flask import Flask, request, jsonify, render_template, url_for
from flask.ext.restful import reqparse, abort, Api, Resource
from flask.ext.cors import CORS

app = Flask(__name__)
cors = CORS(app)
api = Api(app)

#_client_handle = dgc._client_handle

def abort_if_artifact_doesnt_exist(artifact_id):
    if artifact_id not in ['xxxx']: # to be implemented
        abort(404, message="Artifact {} doesn't exist".format(artifact_id))


@app.route('/')
def homePage():
    return render_template("index.html")


@app.route('/checkin')
def checkinPage():
    return render_template("check_in_artifact.html")

@app.route('/checkout')
def checkoutPage():
    return render_template("check_out_artifact.html")


@app.route('/create_link')
def createLink():
    return render_template("create_link.html")


## JSON Tree

@app.route('/get_children_json')
def getChildrenJSON():
    return render_template("children_json_tree.html")


@app.route('/get_parent_json')
def getParentJSON():
    return render_template("parent_json_tree.html")


@app.route('/get_all_json')
def getAllJSON():
    return render_template("all_json_tree.html")


## Graph Tree

@app.route('/get_all_graph')
def getAllGraph():
    return render_template("all_graph_tree.html")


@app.route('/get_children_graph')
def getChildrenGraph():
    return render_template("children_graph_tree.html")


@app.route('/get_parent_graph')
def getParentGraph():
    return render_template("parent_graph_tree.html")



class Artifact(Resource):
    def get(self, artifact_id):
        _return_json = dgc._do_action_on_artifact('get', None, artifact_id)
        return jsonify(_return_json)

    def delete(self, artifact_id):
        return {"message": "SLB: Nothing implemented as yet for delete request on this endpoint"}

    def put(self, artifact_id):
        return {"message": "SLB: Nothing implemented as yet for put request on this endpoint"}


class ArtifactList(Resource):
    def get(self):
        return {"message": "SLB: Nothing implemented as yet for get request on this endpoint"}

    def post(self):
        _return_json = dgc._do_action_on_artifact('put', request.json, None)
        return jsonify(_return_json)

    def put(self):
        _return_json = dgc._do_action_on_artifact('put', request.json, None)
        return jsonify(_return_json)


class ArtifactTreeAll(Resource):
    def get(self, artifact_id):
        _return_json = dgc._do_action_on_relation('all', artifact_id)
        return jsonify(_return_json)


class ArtifactTreeChildren(Resource):
    def get(self, artifact_id):
        _return_json = dgc._do_action_on_relation('children', artifact_id)
        return jsonify(_return_json)


class ArtifactTreeParent(Resource):
    def get(self, artifact_id):
        _return_json = dgc._do_action_on_relation('parent', artifact_id)
        return jsonify(_return_json)


class ArtifactLinkUpdate(Resource):
    def get(self, link_type, parent_artifact_id, children_artifact_id):
        _return_json = dgc._do_action_on_link(link_type, parent_artifact_id, children_artifact_id)
        return jsonify(_return_json)


class ReadJsonDoc(Resource):
    def get(self, json_file):
        _return_json = dgc._do_action_on_json(json_file)
        return jsonify(_return_json)


# Adding rest call api resources
api.add_resource(ArtifactList, '/api/v1/artifact')
api.add_resource(Artifact, '/api/v1/artifact/<string:artifact_id>')
api.add_resource(ArtifactTreeAll, '/api/v1/artifact/tree/all/<string:artifact_id>')
api.add_resource(ArtifactTreeChildren, '/api/v1/artifact/tree/children/<string:artifact_id>')
api.add_resource(ArtifactTreeParent, '/api/v1/artifact/tree/parent/<string:artifact_id>')
api.add_resource(ArtifactLinkUpdate, '/api/v1/artifact/link/<string:link_type>/<string:parent_artifact_id>/<string:children_artifact_id>')
api.add_resource(ReadJsonDoc, '/api/v1/getjson/<string:json_file>')


if __name__ == '__main__':
    app.run('0.0.0.0',port=8000)

