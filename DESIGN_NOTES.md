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

## Handling Special Processing Instructions

Some processing instructions should be sent to every outbox.  Let's say the processing instruction is half way 
through a list of 20 events. Let's say there are 10 regular events before it and 9 after it.   If there is only one 
outbox, we just send the whole batch and no matter how many are actually received, the reverse lookup will give the 
correct number of input events to acknowledge.  

What do we do if there are multiple outboxes ?  We send the first 10 regular events to the selected outbox and 
we just acknowledge those 10.  That will leave events in the in_out_buffer that feeds this edge router.  Next 
time the worker is called, we see that the first event is a processing instruction that needs to be sent to 
every outbox.  We go ahead and enqueue it in every outbox and if that works, we return 1 from the router.enqueue 
method. If we do not successfully deliver to one or more outboxes then we return 0, _which means that processing 
instruction will be re-delivered to one or more nodes_.  

## Duplicate Delivery

In cases where a single batch of events must be split up and delivered to different downstream workers, as with key 
based routing, a backup in one downstream worker could create a situation where events are re-delivered to some 
downstream workers. To prevent actually re-processing these events, the following mechanism is used.  Each worker has 
a unique id and a sequence number. The sequence number only increases and is carried on the envelope of the event.  
When events are enqueued (with the enqueue method) the sender id and sequence number will be in the envelop.  The 
recipient will use the sender id as a dictionary key to look up the highest number seen.  Events less than or equal to 
that number will not be processed but they will be counted in the return value of enqueue.

Sources will emit events that have been numbered but the events they read will generally not have sequence numbers 
so if the source system (e.g. database) sends duplicate events, the source worker will not know and will treat them 
as separate (though identical) events.

## Routing by Key

Each engine will be started with a unique number which we will call the node number.  If there are N engines then the 
unique numbers must be in the range 0 to N-1. Failing to do this will cause events to be missed.  When inboxes are 
created, the address returned will include the node number.  

