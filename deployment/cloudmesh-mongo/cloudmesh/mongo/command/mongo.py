from __future__ import print_function
from cloudmesh.shell.command import command
from cloudmesh.shell.command import PluginCommand
from cloudmesh.common.console import Console
from cloudmesh.common.util import path_expand
from pprint import pprint
from cloudmesh.common.debug import VERBOSE
from cloudmesh.shell.command import map_parameters
from cloudmesh.common.parameter import Parameter

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

        if arguments.deploy and arguments.ips:

            names = Parameter.expand(arguments.names)
            ips = Parameter.expand(arguments.ips)

            if len(ips) != len (names):
                Console.error("The length of ips and names do not match")

            print(f"configure")
            print (ips)
            print(names)
            print(arguments.shards)
            print(arguments.replicas)

        else:
            Console.error("parameters not specified correctly")

        return ""
