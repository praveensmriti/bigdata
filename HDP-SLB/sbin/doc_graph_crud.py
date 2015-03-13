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


def _do_action(action_type):
    pass


def parse_cl_options():
    parser = argparse.ArgumentParser(
        prog='SLB WellSurvey Artifact Document and Graph Management',
        formatter_class=lambda prog: argparse.HelpFormatter(
            prog,
            max_help_position=100),
        description="Module for SLB Artifact Management")

    parser.add_argument(
        "-i",
        "--checkin",
        dest="checkin_doc",
        required=False,
        help="Specify the checkin JSON document file")
    parser.add_argument(
        "-o",
        "--checkout",
        dest="artifact_name",
        required=False,
        help="Specify the artifact name to be checked out")

    return parser.parse_args()


def main():
    args = parse_cl_options()
    success_flag = _do_action(args.src_loc)

    if success_flag:
        print color("Successfully transferred merged file [ {0} ] to S3 bucket [ {1} ]".format(
            args.tmpfile, args.tgt_loc), 'green')
    else:
        print color("Error transfering merged file [ {0} ] to S3 bucket [ {1} ]".format(
            args.tmpfile, args.tgt_loc), 'red')


if __name__ == "__main__":
    main()
