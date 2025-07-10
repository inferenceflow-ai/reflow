## Network Interfaces

Each flow engine will expose a network interface for job management as well as one for each inbox.  Running workers 
send their output to downstream tasks using ZeroMQ connections.  When the upstream and downstream workers are 
in the same process, we want them to not use ZeroMQ at all and just access the queue using plain Python.  Similarly, 
if the workers are on the same host, we want them to use the IPC channel instead of TCP.  

During deployment, each worker sends back it's inbox address to the deployer.  The deployer collects up all of 
the inbox addresses  for each stage and hands them to the upstream stage while it is being deployed so, for a pair 
of connected stages, we have at least the possibility of every upstream worker sending to every downstream worker.

Although the idea of having each task assigned a port doesn't work because multiple workers for a task will be 
on the same host.  Workers in different processes cannot listen on the same port. 

Instead, we will let workers pick their port (ZeroMQ supports this).  Once a worker has picked a port, it will 
register itself locally and will also start an IPC listener on inbox_nnnn and a TCP listener on ip_address:nnnn.  When 
deploying a worker, the "preferred network" will also be passed. When the worker sets up it's TCP listener, it 
will interrogate the available network interfaces and connect on the one matching the preferred network. It 
will then return a tuple of its bind address and port.  Note that workers for different tasks could have the 
same port number on different hosts.  Port number alone does not identify a worker.  However, different workers on the 
same host will never have the same port number because they would have to bind to the same IP address.  That means, 
on a given host, port number does identify a specific worker.     

The deployer will collect up all of this information , consisting of a list of (bind_address, port) tuples, one for 
each engine.  When the upstream task is deployed, this information will then be sent.  As part of worker deployment, 
the local network will be interrogated to determine the preferred IP address.  When wiring up outboxes, it will 
check to see whether the target outbox is on the same host (e.g. it's bind address matches the preferred ip address).
If it is, it will first look for "inbox_nnnn" locally.  It may not be there because that particular worker might 
be in a different process on the same host.  If it is there, it will use a "local" binding to that outbox.  If 
it is not there, it will use the "IPC" binding to that outbox.  Finally, if the outbox is on a different host
(the ip address is not on this host), it will connect using the TCP binding.
