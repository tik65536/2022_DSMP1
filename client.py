#!/usr/bin/python3
import rpyc
from rpyc.utils.registry import REGISTRY_PORT, DEFAULT_PRUNING_TIMEOUT
from rpyc.utils.registry import UDPRegistryClient
from rpyc.utils.factory import DiscoveryError
import sys

cmd = sys.argv[1]
STATE={0: "DO-NOT-WANT" , 1 : "WANTED" , 2: "HELD"}

if(cmd.lower()=="list"):
    try:
        registrar = UDPRegistryClient(port=REGISTRY_PORT)
        list_of_servers = registrar.discover("RA")
        for server in list_of_servers:
            conn = rpyc.connect(server[0],server[1])
            state = conn.root.list_()
            print(f"ID:{state[0]:2d} State:{STATE[state[1]]}")
    except DiscoveryError:
        print(f"DiscoveryError :{DiscoveryError}")
if(cmd.lower()=="time-cs"):
    value=sys.argv[2]
    try:
        registrar = UDPRegistryClient(port=REGISTRY_PORT)
        list_of_servers = registrar.discover("RA")
        for server in list_of_servers:
            conn = rpyc.connect(server[0],server[1])
            state = conn.root.updateHeldTimeout(value)
    except DiscoveryError:
        print(f"DiscoveryError :{DiscoveryError}")
if(cmd.lower()=="time-p"):
    value=sys.argv[2]
    try:
        registrar = UDPRegistryClient(port=REGISTRY_PORT)
        list_of_servers = registrar.discover("RA")
        for server in list_of_servers:
            conn = rpyc.connect(server[0],server[1])
            state = conn.root.updateTimeout(value)
    except DiscoveryError:
        print(f"DiscoveryError :{DiscoveryError}")
