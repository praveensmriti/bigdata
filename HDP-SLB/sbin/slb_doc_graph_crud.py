#!/usr/bin/env python

import argparse
import sys
import json
from time import time
from ConfigParser import ConfigParser
from termcolor import colored as color
import os
from pyorient import OrientDB
import pyorient

_upsert_string = "update {} content {} upsert return after @rid where name = '{}'"
_select_string = "select from {}  where name = '{}'"
_link_artifact = "create edge Link set name = 'Explicit' from (select from V where name ='{}') to {}"
_get_string = "select from (select @rid from V where name = '{}')"
                    

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


def _get_client_connection(conf_dict):
    try:
        HOST = conf_dict['orientdb_server_host']
        PORT = int(conf_dict['orientdb_server_port'])
        USER = conf_dict['orientdb_server_user']
        PASSWORD = conf_dict['orientdb_server_password']
        _client_handle = OrientDB(HOST, PORT)
        _client_handle.connect(USER, PASSWORD)
    except Exception as e:
        print color("Error getting OrientDB connection : " + e.message, 'red')
        sys.exit(1)
    return _client_handle


def _create_db_graph_objects(c_handle):
    try:
        # Create SLB graph database
        c_handle.db_create("WellSurveyGraph", pyorient.DB_TYPE_GRAPH, pyorient.STORAGE_TYPE_PLOCAL)
        c_handle.db_open("WellSurveyGraph", "admin", "admin")

        # Create SLB graph vertex and edges
        c_handle.command('create class Folder extends V')
        #c_handle.command('create property Folder.name STRING')
        #c_handle.command('alter property Folder.name MANDATORY true')
        c_handle.command('insert into Folder set name ="folder1"')

        c_handle.command('create class WellCollection extends V')
        #c_handle.command('create property WellCollection.name STRING')
        #c_handle.command('alter property WellCollection.name MANDATORY true')
        c_handle.command('insert into WellCollection set name ="wellcollection1"')

        c_handle.command('create class Well extends V')
        #c_handle.command('create property Well.name STRING')
        #c_handle.command('alter property Well.name MANDATORY true')
        c_handle.command('create class Survey extends V')
        #c_handle.command('create property Survey.name STRING')
        #c_handle.command('alter property Survey.name MANDATORY true')
        c_handle.command('create class Log extends V')
        #c_handle.command('create property Log.name STRING')
        #c_handle.command('alter property Log.name MANDATORY true')
        c_handle.command('create class Link extends E')
        #c_handle.command('create property Link.LinkType STRING')
        #c_handle.command('alter property Link.LinkType MANDATORY true')

        c_handle.command('create edge Link set LinkType = "Explicit" from (select from WellCollection) to (select from Folder)')

    except Exception as e:
        print color("Error creating graph database objects : " + e.message, 'red')
        sys.exit(1)
    return 'Success'


def _put_json_doc(client_handle, json_string):

    _json_data = json.loads(json_string)
    _parent_name = _json_data['parent_name']
    _artifact_type = _json_data['artifact_type']
    _artifact_name = _json_data['payload']['name']
    _data = _json_data['payload']['data']
   
    try:  
        _command_string = _upsert_string.format(_artifact_type, _data, _artifact_name)
        _response_rid = client_handle.command(_command_string)
    except Exception as e:
        print color("Error updating artifact : " + e.message, 'red')
        sys.exit(1)

    try:
        _command_string = _link_artifact.format(_parent_name, _response_rid)
        _return = client_handle.command(_command_string)
    except Exception as e:
        print color("Error updating artifact edge/link: " + e.message, 'red')
        sys.exit(1)
         
    return {"response" : _response_rid}


def _get_artifact(client_handle, artifact_name):
    _command_string = _get_string.format(artifact_name) 
    _doc = client_handle.command(_command_string)[0]
    return _doc.__dict__['_OrientRecord_o_storage'] 


def _do_action_on_artifact(action_type, json_string=None, artifact_name=None):
    if action_type in 'put':
        _record_id = _put_json_doc(_client_handle, json_string)
        #print 'Created record {}'.format(_record_id)
        return _record_id
    elif action_type in 'get':
        _json_doc = _get_artifact(_client_handle, artifact_name)
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
        dest="artifact_name",
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
        "--parent-name",
        dest="parent_name",
        required=False,
        help="Specify the parent name for the artifact")

    return parser


def main():
    parser = parse_cl_options()
    args = parser.parse_args()
    if args.checkin_doc or args.artifact_name:
        success_flag = _do_action(args)
    else:
        parser.print_help()


try:
    _config_dict = _initialize_credentials()
    _client_handle = _get_client_connection(_config_dict)
    _client_handle.db_open("WellSurveyGraph", "admin", "admin")
except Exception as e:
    print color("Error initializing SLB DB credential/connections: " + e.message, 'red')
    sys.exit(1)


if __name__ == "__main__":
    main()
