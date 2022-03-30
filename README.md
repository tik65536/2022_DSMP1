# 2022_DSMP1
Distribution System miniProject 1
## Summary 
Server.py <N> where N is the number of RA node
  The program will create N Rpyc Thread Server that register with the UDP Registery Server.
  Each of the Thread Server will have a inner class backend as a separated thread to process on state change while the thread server is handling the request and reply message though RPC call.

client.py <list|time-cs (int)|time-p (int)>
  The program will look for register server to get a list of node that running RA service and issue the corresponing cmd to each server.
