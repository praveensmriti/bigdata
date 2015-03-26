#!/usr/bin/env python

import argparse
import sys, os
import json
from time import time
from ConfigParser import ConfigParser
from termcolor import colored as color
import requests as rq
import os
from pyorient import OrientDB
import pyorient

'''final_url = url_values[category_name]
resp = requests.get(final_url, auth=(cm_username, cm_password))
dict_list = resp.json().get("items") '''

_base_url  = 'http://slb-orient.cloudapp.net:2480/query/WellSurveyGraph/sql/{}'
_base_url  = 'http://{}:{}/query/{}/sql/{}'
_base_url1 = 'http://{hostname}:{port}/query/{database-name}/sql/{query-string}'


# Skeleton strings
_upsert_string         = "update {} content {} upsert return after @rid where aid = '{}'"
_select_string         = "select from {}  where aid = '{}'"
_rid_string            = "select  from V where aid = '{}'" 
_type_string           = "select  from V where @class = '{}'" 
_link_artifact_final1  = "update Link set name = 'Explicit', out={},in={} upsert where out={} and in={}"
_link_artifact_final   = "create edge Link set LinkType = 'Explicit' from {} to {}"
_link_exists           = "select from Link where out={} and in={} and LinkType ='Explicit'"
_link_exists_c         = "select from Link where in={} and LinkType ='Explicit'"
_get_string            = "select expand( @this.exclude('out_Link').exclude('in_Link')) from V where aid = '{}'"

_relation_string_base  = "select expand(@this.exclude('out_Link').exclude('in_Link')) from (traverse "
_relation_string       = { "all"      : _relation_string_base + "* from {}) where @class not in 'Link'",
                           "children" : _relation_string_base + "out('Link') from {}) where @class not in 'Link'",
                           "parent"   : _relation_string_base + "in('Link') from {}) where @class not in 'Link'" }



def _get_config_handle():
    try:
        _config_handle = ConfigParser()
        _config_handle.read('conf/slb-main.conf')
        return _config_handle
    except Exception as e:
        print "Error getting config handle : {0}".format(e.message)
        sys.exit(1)


def _initialize_credentials():
    try:
        _config_dict = {}
        for option, value in _get_config_handle().items('MAIN'):
            _config_dict[option] = value
        return _config_dict
    except Exception as e:
        print "Error initializing credentials : {0}".format(e.message)
        sys.exit(1)


def _modify_config(section_name, config_name_value_dict):
    try:
        _config_handle = _get_config_handle()
        for config_name, config_value in config_name_value_dict.iteritems():
            _config_handle.set(section_name, config_name, config_value)
        file_handle = open('conf/slb-main.conf', 'w')
        _config_handle.write(file_handle)
        file_handle.close()
    except Exception as e:
        print "Error setting config {0} : {1}".format(config_name, e.message)
        sys.exit(1)


'''orientdb_host_ip      = 192.168.56.101
orientdb_host_name    = hdpclient.localdomain
orientdb_binary_port  = 2424
orientdb_rest_port    = 2480

orientdb_user         = root
orientdb_password     = slbroot

slb_database          = WellSurveyGraph
slb_database_user     = admin
slb_database_password = admin

o_slb_base_rest_url   = http://%(orientdb_host_name)s:%(orientdb_rest_port)s/query/%(slb_database)s/sql/{} '''




def _get_client_connection(conf_dict):
    try:
        HOST = conf_dict['orientdb_host_name']
        PORT = int(conf_dict['orientdb_binary_port'])
        USER = conf_dict['orientdb_user']
        PASSWORD = conf_dict['orientdb_password']
        _client_handle = OrientDB(HOST, PORT)
        _client_handle.connect(USER, PASSWORD)
    except Exception as e:
        print color("Error getting OrientDB connection : " + e.message, 'red')
        sys.exit(1)
    return _client_handle


def _create_db_graph_objects(c_handle, conf_dict):
    try:
        if conf_dict['slb_db_create'] == 'yes':
            _slb_db = conf_dict['slb_database']
            _slb_db_user = conf_dict['slb_database_user']
            _slb_db_pwd = conf_dict['slb_database_password']

            # Create SLB graph database
            c_handle.db_create(_slb_db, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
            c_handle.db_open(_slb_db, _slb_db_user, _slb_db_pwd)

    except Exception as e:
        print color("Error creating graph database objects : " + e.message, 'red')
        sys.exit(1)
    return 'Success'



def _put_json_doc(client_handle, json_string):
    
    try:
        _json_data = json_string

        #_parent_id = _json_data['parent_id']

        if 'parent_id' not in _json_data:
            _parent_id = None
            _parent_rid = None
        else:
            _parent_id = _json_data['parent_id']

        if _parent_id is not None:
            _parent_list = client_handle.command(_rid_string.format(_parent_id))
            if len(_parent_list) > 0:
                _parent_rid = _parent_list[0].rid
            else:
                return __message("Parent artifact {} does not exists".format(_parent_id))

        _artifact_type = _json_data['artifact_type']
        if _artifact_type is not None:
            _type_list = client_handle.command(_type_string.format(_artifact_type))
            if len(_type_list) == 0:
                return __message("Artifact type {} does not exists".format(_artifact_type))
        else:
            return __message("Artifact type is null")

        _artifact_id = _json_data['payload']['name']
        _data = json.dumps(_json_data['payload'])
    except Exception as e:
        return __message("Error parsing json string : " + e.message) 
   
    try:  
        _command_string = _upsert_string.format(_artifact_type, _data, _artifact_id)
        _response = client_handle.command(_command_string)
        _rid = str(_response[0]).replace('##','#')
    except Exception as e:
        return __message("Error updating artifact : " + e.message)

    try:
        #_parent_rid = client_handle.command(_rid_string.format(_parent_id))[0].rid
        _link_count = []
        if _parent_rid is not None:
            _link_count = client_handle.command(_link_exists.format(_parent_rid, _rid))

        if len(_link_count) ==  0 and _parent_id is not None :
            _command_string = _link_artifact_final.format(_parent_rid, _rid)
            _return = client_handle.command(_command_string)
    except Exception as e:
        return __message("Error updating artifact edge/link: " + e.message)
    return __message("Record ID is " +  _rid)



def _get_artifact(client_handle, artifact_id):

    _command_string = _get_string.format(artifact_id) 
    _doc = client_handle.command(_command_string)
    _record_message = {'SLB-Message': 'Artifact {} does not exists'.format(artifact_id)}

    if len(_doc) > 0:
        _record_message = _doc[0].oRecordData
    return _record_message



def __message(message):
    return {'SLB-Message' : message}


def _validate_artifact(artifact_id):
    _dict = {}
    _command_string = _rid_string.format(artifact_id)
    _command_status = _client_handle.command(_command_string)
    
    if len(_command_status) > 0:
        #return __message(_command_status[0]._OrientRecord__rid)
        #_dict['rid'] = _command_status[0].rid
        _dict['rid'] = _command_status[0]._OrientRecord__rid
    else:
        return __message('Artifact {} does not exist'.format(artifact_id))

    return _dict


def _do_action_on_json(json_file):
    try:
        SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
        json_url = os.path.join(SITE_ROOT, "data", json_file)
        if not os.path.exists(json_url):
            return __message('File {} does not exists to be displayed'.format(json_file))
        _data = json.load(open(json_url))
    except Exception as e:
        return __message("Error loading json file : " + e.messasge)
    return _data


def _do_action_on_relation(action_type, artifact_id):

    _exists_status = _validate_artifact(artifact_id)

    if 'rid' not in _exists_status:
        return _exists_status

    _rid = _exists_status['rid']
    _command_string = _relation_string[action_type].format(_rid)

    try:
        _traverse_dict = {'SLB-Artifact-Tree':[]}
        _command_status = _client_handle.command(_command_string)
        for i in _command_status:
            _artifact_json = {}
            try:
                _artifact_json[i.oRecordData['name']] = i.oRecordData
            except:
                _artifact_json['tempname'] = str(i.oRecordData)
            _traverse_dict['SLB-Artifact-Tree'].append(_artifact_json) 

        #return __message(str(_command_status[0].oRecordData))
        return _traverse_dict
    except Exception as e:
        return __message('Error executing relation command : ' + e.message)


def _do_action_on_artifact(action_type, json_string=None, artifact_id=None):

    if action_type in 'put':
        _json_doc = _put_json_doc(_client_handle, json_string)
        return  _json_doc

    elif action_type in 'get':
        _json_doc = _get_artifact(_client_handle, artifact_id)
        return _json_doc 


def parse_cl_options():
    parser = argparse.ArgumentParser(
        prog='SLB WellSurvey Artifact Document and Graph Management',
        formatter_class=lambda prog: argparse.HelpFormatter(
            prog,
            max_help_position=100),
        description="Module for SLB Artifact Management")

    parser.add_argument(
        "--checkin",
        dest="checkin_doc",
        required=False,
        help="Specify the checkin JSON document file")
    parser.add_argument(
        "--checkout",
        dest="artifact_id",
        required=False,
        help="Specify the artifact name to be checked out")
    parser.add_argument(
        "--checkin-type",
        dest="checkin_type",
        required=False,
        help="Specify the artifact checkin artifact type e.g wells or survey")
    parser.add_argument(
        "--checkout-type",
        dest="checkout_type",
        required=False,
        help="Specify the artifact checkout artifact type e.g wells or survey")
    parser.add_argument(
        "--parent-id",
        dest="parent_id",
        required=False,
        help="Specify the parent name for the artifact")
    return parser


def main():
    parser = parse_cl_options()
    args = parser.parse_args()

try:
    _config_dict = _initialize_credentials()
    _client_handle = _get_client_connection(_config_dict)

    #_db_status = _create_db_graph_objects(_client_handle, _config_dict)

    _slb_db = _config_dict['slb_database']
    _slb_db_user = _config_dict['slb_database_user']
    _slb_db_pwd = _config_dict['slb_database_password']
    _client_handle.db_open(_slb_db, _slb_db_user, _slb_db_pwd)

except Exception as e:
    print color("Error initializing SLB DB credential/connections: " + e.message, 'red')
    sys.exit(1)


if __name__ == "__main__":
    main()
