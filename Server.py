#!/usr/bin/python3
import rpyc
from rpyc.utils.server import ThreadedServer
import datetime
from rpyc.utils.registry import REGISTRY_PORT, DEFAULT_PRUNING_TIMEOUT
from rpyc.utils.registry import UDPRegistryServer
from rpyc.utils.factory import DiscoveryError
from threading import Thread
import threading
from time import sleep
import numpy as np
import sys
import socket

STATE={0: "DO-NOT-WANT" , 1 : "WANTED" , 2: "HELD"}
class RAService(rpyc.Service):

    class backend(Thread):
        def __init__(self,_id,nodes,ip,port,heldtimeout=0,timeout=0):
            Thread.__init__(self)
            self.STATE=0
            self._id=_id
            self.heldtimeout = heldtimeout
            self.timeout = timeout
            self.RANodes=[]
            self.nodes=nodes
            self.cmdtimesnap=datetime.datetime.now()
            self.heldtimesnap=0
            self.waitForResponse=[]
            self.waitForReply=0
            self.ip = ip
            self.port = port
            self.lamport = 0
            self.requestsnap=-1
            self.mutex = threading.Lock()

        def increaselamport(self):
            self.mutex.acquire()
            self.lamport+=1
            self.mutex.release()

        def stateMachine(self):
            now = datetime.datetime.now()
            if(self.STATE==0 and ((now-self.cmdtimesnap).seconds >abs(self.timeout-5))):
                # STATE 0 to 1
                if(np.random.randint(0,2) or (now-self.cmdtimesnap).seconds >=self.timeout):
                    self.increaselamport()
                    self.mutex.acquire()
                    self.STATE=1
                    self.requestsnap=self.lamport
                    self.cmdtimesnap=now
                    self.mutex.release()
                    print(f'ID:{self._id:2d} Change States {STATE[self.STATE]:11s}')
                    self.waitForReply=len(self.RANodes)-1
                    for i in self.RANodes:
                        if(i[0]!=self.ip and i[1]!=self.port):
                            conn = rpyc.connect(i[0],i[1])
                            self.increaselamport()
                            req = rpyc.async_(conn.root.Request)
                            req(self.requestsnap,self.ip,self.port)
                else:
                    print(f'ID:{self._id:2d} Keep States {STATE[self.STATE]:11s}')
            if(self.STATE==2 and ((now-self.heldtimesnap).seconds>abs(self.heldtimeout-10))):
                if(np.random.randint(0,2) or ((now-self.heldtimesnap).seconds>=self.heldtimeout)):
                    self.increaselamport()
                    self.mutex.acquire()
                    self.STATE=0
                    self.requestsnap=-1
                    self.cmdtimesnap=now
                    self.mutex.release()
                    print(f'ID:{self._id:2d} Release States {STATE[self.STATE]:11s}')
                    self.sendReply()

        def sendReply(self):
            print(f'ID:{self._id:2d} Release States Send Deferred Reply {len(self.waitForResponse):3d}')
            for i in self.waitForResponse:
                conn = rpyc.connect(i[0],i[1])
                self.increaselamport()
                rep = rpyc.async_(conn.root.Reply)
                rep(self.lamport)
            self.waitForResponse=[]

        def run(self):
            while True:
                try:
                    self.RANodes = rpyc.discover("RA")
                    if(len(self.RANodes)==self.nodes):
                        print(f'ID:{self._id:2d} RA node length :{len(self.RANodes)}, Lamport : {self.lamport:8d},State : {STATE[self.STATE]:11s}, waitRespons:{len(self.waitForResponse):2d}, waitForReply:{self.waitForReply:2d}')
                        self.stateMachine()
                        sleep(2)
                except DiscoveryError:
                        print(f'ID:{self._id:2d} DiscoveryError: {DiscoveryError}')

    # end of inner class

    def __init__(self,_id,nodes,ip,port):
        super(self.__class__, self).__init__()
        self.b = self.backend(_id, nodes, ip, port)
        self.mutex = threading.Lock()
        self.b.start()

    def increaselamport(self,lamport=0):
        self.mutex.acquire()
        self.b.lamport=max(self.b.lamport,lamport)+1
        self.mutex.release()

    def exposed_Request(self,lamport,ip,port):
        #self.increaselamport(lamport)
        print(f'ID:{self.b._id:2d} Request: fr:{port:5d}, my:{self.b.port:5d}, lamport:{lamport:d}, myRequestlampot:{self.b.requestsnap:4d}')
        #if(self.b.STATE!=2 and (self.b.STATE==1 and ( (lamport<self.b.requestsnap or self.b.requestsnap==-1) or \
        #                                            (lamport==self.b.requestsnap and port<self.b.port )))):
        if(self.b.STATE==2 or (self.b.STATE==1 and ((lamport>self.b.requestsnap) or (lamport==self.b.requestsnap and port>self.b.port)))):
            print(f'ID:{self.b._id:2d} Request: fr:{port:5d}, my:{self.b.port:5d} DEFERRED ')
            self.b.waitForResponse.append([ip,port])
        else:
            self.increaselamport(lamport)
            conn = rpyc.connect(ip,port)
            print(f'ID:{self.b._id:2d} Request: fr:{port:5d}, my:{self.b.port:5d} GRANT ')
            self.increaselamport()
            rep = rpyc.async_(conn.root.Reply)
            rep(self.b.lamport)

    def exposed_Reply(self,lamport):
        self.increaselamport(lamport)
        if(self.b.waitForReply<=0):
            print(f'ID:{self.b._id:2d} Receive Reply Error waitForReply ==0 ')
            return
        else:
            self.b.waitForReply-=1
            print(f'ID:{self.b._id:2d} Receive Reply Remain {self.b.waitForReply}')
        if(self.b.waitForReply==0):
            print(f'ID:{self.b._id:2d} Receive Reply All GRANT')
            self.increaselamport()
            self.mutex.acquire()
            self.b.STATE=2
            self.b.cmdtimesnap = datetime.datetime.now()
            self.b.heldtimesnap = datetime.datetime.now()
            self.mutex.release()
            print(f'ID:{self.b._id:2d} Change States {STATE[self.STATE]:11s}')

    def exposed_updateHeldTimeout(self,data):
        self.b.heldtimeout=int(data)
        print(f'ID:{self.b._id:2d} Update heldTimeout  {self.b.heldtimeout}')


    def exposed_updateTimeout(self,data):
        self.b.timeout=int(data)
        print(f'ID:{self.b._id:2d} Update Timeout  {self.b.timeout}')

    def exposed_list_(self):
        return self.b._id,self.b.STATE

class RegistryService():
    def __init__(self):
        self.server = UDPRegistryServer(port=REGISTRY_PORT,pruning_timeout=DEFAULT_PRUNING_TIMEOUT)

    def start(self):
        self.server.start()

if __name__=='__main__':
    if(len(sys.argv)<2):
        print("Missing Args")
        sys.exit(1)
    N = int(sys.argv[1])
    hostname = socket.gethostname()
    ip = socket.gethostbyname(hostname)
    registry = RegistryService()
    t=Thread(target=registry.start)
    t.daemon=True
    t.start()
    startport = np.random.randint(1024,65536)
    ports = np.array([ startport+i for i in range(N) ])
    np.random.shuffle(ports)
    print(ports)
    for i in range(N):
        print(i)
        service = RAService(i,N,ip,int(ports[i]))
        server=ThreadedServer(service, port=int(ports[i]),auto_register=True)
        t = Thread(target=server.start)
        t.daemon=True
        t.start()



