from __future__ import print_function

import paramiko
import yaml
from cloudmesh.common.console import Console
from cloudmesh.common.parameter import Parameter
from cloudmesh.shell.command import PluginCommand
from cloudmesh.shell.command import command
from cloudmesh.shell.command import map_parameters

SERVER_TYPE_CONFIG = "configsvr"
SERVER_TYPE_MONGOS = "mongos"
SERVER_TYPE_DATA = "data"


def parse_config(file_name):
    with open(file_name, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            Console.error("Error in parsing the configuration file")


def init_ssh(ip, username):
    client = paramiko.SSHClient()
    client.load_system_host_keys()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(ip, username=username)
    return client


def validate_connection(nodes_arr, username, id):
    print("Validating connections to " + id + " servers")
    for ip in nodes_arr:
        # validate connections
        print("Testing the connection to " + ip)
        try:
            client = init_ssh(ip, username)
            # connection is OK, verify the availability of monngod
            stdin, stdout, stderr = client.exec_command("mongod --version")
            found_mongodb = False
            for line in stdout:
                if "db version" in line.strip('\n'):
                    print("Found " + line.strip('\n') + " in " + ip)
                    found_mongodb = True
                    break
            if not found_mongodb:
                Console.error("Couldn't find mongodb in " + ip)
                return False
        except Exception as e:
            Console.error("Failed to connect to " + ip + " due to " + str(e))
            return False
        client.close()
    return True


def mkdir(ip, ssh_client: paramiko.SSHClient, dirs=[]):
    for dir in dirs:
        print("Creating dir " + dir + " on " + ip)
        stdin, stdout, stderr = ssh_client.exec_command("mkdir -p " + dir)
        for line in stdout:
            print("Failed to create the directory " + dir + " on " + ip)
            return False
    return True


def start_db(client: paramiko.SSHClient, ip, port, repl_name, db_path, db_type):
    print("Starting db on " + ip + ":" + str(port))

    db_prefix = db_type + str(port)
    db_data = db_path + "/data/" + db_prefix
    db_logs = db_path + "/logs/" + db_prefix

    created = mkdir(ip, client, [db_data, db_logs])
    if not created:
        return False

    db_start_cmd = "mongod --configsvr --replSet %s --dbpath %s --port %d --fork --logpath %s" % (
        repl_name, db_data, port, db_logs + "/logs.log")
    stdin, stdout, stderr = client.exec_command(db_start_cmd)
    status = stdout.channel.recv_exit_status()
    if status is 0:
        print("Database started successfully.")
        return True
    elif status is 48:
        print("Database is already running on " + ip + ":" + str(port))
        return True
    else:
        # todo handle other status codes
        return False


def start_config_servers(servers, username, ports_offset, db_path):
    current_port = ports_offset
    for server in servers:
        client = init_ssh(server, username)
        # start the database
        started = start_db(client, servers, ports_offset, "configrpl", db_path, SERVER_TYPE_CONFIG)
        if not started:
            return False

        current_port = current_port + 1

        if current_port is (ports_offset + len(servers)):
            # if this is the last server
            # check the replication status
            stdin, stdout, stderr = client.exec_command("mongo --port %d --eval \"rs.status().code\"", current_port - 1)

    return True


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
    common_auth = config["nodes"]["auth"]
    connections = {
        SERVER_TYPE_MONGOS: [mongos],
        SERVER_TYPE_CONFIG: config_nodes,
        SERVER_TYPE_DATA: data_nodes
    }

    for key in connections:
        valid = validate_connection(connections[key], common_auth["username"], key)
        if not valid:
            return False

    # at this point all connections are valid and has mongod
    ports_offset = config["nodes"]["port"]
    db_path = config["nodes"]["dbPath"]

    start_config_servers(connections[SERVER_TYPE_CONFIG], common_auth["username"], ports_offset, db_path)
    ports_offset = ports_offset + len(connections[SERVER_TYPE_CONFIG])

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
