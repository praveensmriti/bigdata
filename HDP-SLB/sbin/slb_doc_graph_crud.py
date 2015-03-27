#!/usr/bin/env python

import argparse
import sys, os
import json, requests
from time import time
from ConfigParser import ConfigParser
from termcolor import colored as color
import requests as rq
import os
from pyorient import OrientDB
import pyorient


# Skeleton strings
_upsert_string         = "update {} content {} upsert return after @rid where aid = '{}'"
_select_string         = "select from {}  where aid = '{}'"
_rid_string            = "select  from V where aid = '{}'" 
_type_string           = "select  from V where @class = '{}'" 
_link_artifact_final1  = "update Link set name = 'Explicit', out={},in={} upsert where out={} and in={}"
_link_artifact_final   = "create edge Link set LinkType = 'Explicit' from {} to {}"
_link_exists           = "select from Link where out={} and in={} and LinkType ='Explicit'"
_link_exists_1         = "select @rid from Link where in={} and LinkType ='Explicit'"
_link_exists_c         = "select from Link where in={} and LinkType ='Explicit'"
_get_string            = "select expand( @this.exclude('out_Link').exclude('in_Link')) from V where aid = '{}'"
_exists_string         = "(select from V where aid = '{}')"

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


def _get_client_connection(host, port, user, password):
    try:
        _client_handle = OrientDB(host, port)
        _client_handle.connect(user, password)
    except Exception as e:
        print color("Error getting OrientDB connection : " + e.message, 'red')
        sys.exit(1)
    return _client_handle


def _create_db_graph_objects(c_handle, conf_dict):
    try:
        if conf_dict['orientdb_slb_db_create'] == 'yes':
            _slb_db_create_file = conf_dict['db_create_script_file']
            _batch_url          = conf_dict['o_slb_batch_rest_url']
            
            if c_handle.db_exists(_slb_db):
                #return __message('Database {} already exists'.format(_slb_db))
                pass
            else:
                # Create SLB graph database
                c_handle.db_create(_slb_db, pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
                c_handle.db_open(_slb_db, _slb_db_user, _slb_db_pwd)

                _site_root = os.path.realpath(os.path.dirname(__file__))
                _script_path = os.path.join(_site_root, "script", _slb_db_create_file)

                if not os.path.exists(_script_path):
                    return __message('Script file {} does not exists to be executed'.format(_slb_db_create_file))

                _script_json = json.dumps(json.load(open(_script_path)))
                _resp = _play_http_request(_batch_url, 'post', _script_json)
    except Exception as e:
        print color("Error creating graph database objects : " + e.message, 'red')
        sys.exit(1)
    return __message("DB create module execution is successful")



def _play_http_request(url, verb_type, json_doc=None):
    _headers  = {'content-type': 'application/json'}

    if verb_type == 'post':
        _resp = requests.post(url, data=json_doc, headers=_headers, auth=(_slb_db_user, _slb_db_pwd))
    elif verb_type == 'get':
        _resp = requests.get(url, auth=(_slb_db_user, _slb_db_pwd))
        return _resp.json()

    return __message(str(_resp))
    
_get_rid = 'select @rid as rid from V where aid = {}'


def _put_json_doc(json_string):
    
    try:
        _json_data = json_string
        _json_keys_out = _json_data.viewkeys()
        _base_keys_out = {'parent_aid', 'artifact_type', 'payload'}
        _base_keys_in  = {'aid', 'aname'}
        _artifact_type_list = _config_dict['artifact_type_list'].strip().split(',')

        if not _json_data.viewkeys() >= _base_keys_out: 
            return __message('JSON doc does not have a right format (out base)')
        elif not _json_data['payload'].viewkeys() >= _base_keys_in:
            return __message('JSON doc does not have a right format (in base}')
            
        _parent_aid = _json_data['parent_aid']
        _parent_rid = None

        if _parent_aid is not None:
            _command_string = _get_rid.format(_parent_aid)
            _query_url = _config_dict['o_slb_base_rest_url'].format(_command_string)
            _resp = _play_http_request(_query_url, 'get')

            if len(_resp['result']) > 0:
                _parent_rid = _resp['result'][0]['rid']
            else:
                return __message("Parent artifactID {} does not exists".format(_parent_aid))

        _artifact_type = _json_data['artifact_type']

        if _artifact_type is not None and _artifact_type.lower() in _artifact_type_list:
            return __message("Artifact type is invalid or null")

        _artifact_id = _json_data['payload']['aid']
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
        if _parent_rid is not None:
            _command_string = _link_exists_1.format(_rid)
            _query_url = _config_dict['o_slb_base_rest_url'].format(_command_string)
            _resp = _play_http_request(_query_url, 'get')

            if len(_resp['result']) > 0:
                _edge_rid = _resp['result'][0]['rid']
                _return = _client_handle.command('delete Link')
             
            _command_string = _link_artifact_final.format(_parent_rid, _rid)
            _return = client_handle.command(_command_string)


        '''
        # Link update and creation
        if _parent_rid is not None:
            _link_count = client_handle.command(_link_exists.format(_parent_rid, _rid))

        if len(_link_count) ==  0 and _parent_id is not None :
            _command_string = _link_artifact_final.format(_parent_rid, _rid)
            _return = client_handle.command(_command_string)
        '''

    except Exception as e:
        return __message("Error updating artifact edge/link: " + e.message)
    return __message("Record ID is " +  _rid)


def _get_artifact(client_handle, artifact_id):
    _record_message = {'SLB-Message': 'Artifact {} does not exists'.format(artifact_id)}
    _command_string = _get_string.format(artifact_id)

    _query_url = _config_dict['o_slb_base_rest_url'].format(_command_string)
    _resp = _play_http_request(_query_url, 'get')

    if len(_resp['result']) > 0:
        _resp_dict = _resp['result'][0]
        _resp_dict.pop("@type")
        _resp_dict['artifact_type'] = _resp_dict.pop("@class")
        _resp_dict['current_version'] = _resp_dict.pop("@version")
        _record_message = _resp_dict

    return _record_message


def __message(message):
    return {'SLB-Message' : message}


def _validate_artifact(artifact_id):
    _dict = {}
    _command_string = _rid_string.format(artifact_id)
    _command_status = _client_handle.command(_command_string)
    
    if len(_command_status) > 0:
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


def _do_action_on_relation1(action_type, artifact_id):
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
                _artifact_json[i.oRecordData['aname']] = i.oRecordData
            except:
                _artifact_json['tempname'] = str(i.oRecordData)
            _traverse_dict['SLB-Artifact-Tree'].append(_artifact_json) 

        #return __message(str(_command_status[0].oRecordData))
        return _traverse_dict
    except Exception as e:
        return __message('Error executing relation command : ' + e.message)


def _do_action_on_relation(action_type, artifact_id):
    _record_message = {'SLB-Message': 'Artifact {} does not exists'.format(artifact_id)}
    _command_string = _relation_string[action_type].format(_exists_string.format(artifact_id))

    _query_url = _config_dict['o_slb_base_rest_url'].format(_command_string)
    _resp = _play_http_request(_query_url, 'get')

    if len(_resp['result']) > 0:
        _record_message = _resp

    return _record_message


def _do_action_on_artifact(action_type, json_string=None, artifact_id=None):

    if action_type in 'put':
        _json_doc = _put_json_doc(json_string)
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

    _slb_db = _config_dict['slb_database']
    _slb_db_user = _config_dict['slb_database_user']
    _slb_db_pwd = _config_dict['slb_database_password']

    _host = _config_dict['orientdb_host_name']
    _port = int(_config_dict['orientdb_binary_port'])
    _user = _config_dict['orientdb_user']
    _password = _config_dict['orientdb_password']

    _client_handle = _get_client_connection(_host, _port, _user, _password)

    _db_status = _create_db_graph_objects(_client_handle, _config_dict)
    _client_handle.db_open(_slb_db, _slb_db_user, _slb_db_pwd)

except Exception as e:
    print color("Error initializing SLB DB credential/connections: " + e.message, 'red')
    sys.exit(1)


if __name__ == "__main__":
    main()
