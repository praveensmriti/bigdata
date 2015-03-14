#!/usr/bin/env python

import argparse
import sys
from time import time
from ConfigParser import ConfigParser
from termcolor import colored as color
import os
from pyorient import OrientDB




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

        # Create SLB graph vertexs
        c_handle.command('create class Wells extends V')
        c_handle.command('create class Survey extends V')
        c_handle.command('create class survey_belongs_to extends E')
    except Exception as e:
        print color("Error creating graph database objects : " + e.message, 'red')
        sys.exit(1)
    return 'Success'


def _checkin_json_doc(client_handle, json_string):
    _return_flag = _create_db_graph_objects(client_handle)

    

def _do_action(arguments,checkin_json,artifact_name):
    _config_dict = _initialize_credentials()
    _client_handle = _get_client_connection(_config_dict)
    _record_id = _checkin_json_doc(_client_handle, arguments.checkin_doc, arguments.checkin_type)
    _record_id = _checkout_json_doc(_client_handle, arguments.artifact_name, arguments.checkout_type)

    print 'Processing checkin and checkout of artifacts'

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


if __name__ == "__main__":
    main()
