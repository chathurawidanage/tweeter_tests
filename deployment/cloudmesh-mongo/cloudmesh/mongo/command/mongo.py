from __future__ import print_function

import paramiko
import yaml
from cloudmesh.common.console import Console
from cloudmesh.common.parameter import Parameter
from cloudmesh.shell.command import PluginCommand
from cloudmesh.shell.command import command
from cloudmesh.shell.command import map_parameters


def parse_config(file_name):
    with open(file_name, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            Console.error("Error in parsing the configuration file")


def validate_connection(nodes_arr, username):
    for ip in nodes_arr:
        # validate connections
        print("Testing the connectiom to " + ip)
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        try:
            client.connect(ip, username=username)
        except Exception as e:
            Console.error("Failed to connect to " + ip + " due to " + str(e))
        client.close()


def validate_config(config):
    shard_count = config["shard"]["count"]
    shard_replications = config["shard"]["replications"]
    total_data_nodes = shard_count * shard_replications

    data_nodes = config["nodes"]["data"]
    config_nodes = config["nodes"]["config"]
    mongos = config["nodes"]["mongos"]

    if len(data_nodes) < total_data_nodes:
        Console.error(
            "No of data nodes are not sufficient to support required shard replication. Found " + str(
                len(data_nodes)) + ", Required " + str(total_data_nodes))

    # validate connections

    auth = config["nodes"]["auth"]

    validate_connection([mongos], auth["username"])
    # validate_connection(data_nodes)

    return True


class MongoCommand(PluginCommand):

    # noinspection PyUnusedLocal
    @command
    def do_mongo(self, args, arguments):
        """
        ::

          Usage:
             mongo deploy --config FILE
             mongo deploy --ips=IPS
                          --names=NAMES
                          --shards=SHARDS
                          --replicas=REPLICAS

          This command does some useful things.

          Arguments:
              FILE   a file name

          Options:
              -f      specify the file

          Description:
          >   mongo deploy --config FILE
          >   mongo deploy --ips=10.0.0.[1-5]
          >                --names=master,worker[2-5]
          >                --shards=3
          >                --replicas=2
        """

        map_parameters(arguments,
                       "config",
                       "ips",
                       "names",
                       "replicas")

        if arguments.deploy and arguments.config:
            print(f"configure from file {arguments.FILE}")
            config = parse_config(arguments.FILE)
            valid = validate_config(config)

        if arguments.deploy and arguments.ips:

            names = Parameter.expand(arguments.names)
            ips = Parameter.expand(arguments.ips)

            if len(ips) != len(names):
                Console.error("The length of ips and names do not match")

            print(f"configure")
            print(ips)
            print(names)
            print(arguments.shards)
            print(arguments.replicas)

        else:
            Console.error("parameters not specified correctly")

        return ""
