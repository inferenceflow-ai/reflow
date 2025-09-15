# Overview

REFlow is a pure Python event processing frameworks.  As a REFlow developer, you define a _flow_, which is a graph 
of transformation that you want to apply to incoming events. Each transformation is defined with simple Python classes 
and functions that do not reference the REFlow API at all.  REFLow takes this Pipeline definition and runs it on a 
compute cluster, in essence, turning your pipeline into a very scalable, distributed event processing application. 
REFlow also has features that support event driven services.   More on that later.  First, we need to cover some basic concepts.

# Core Concepts and Definitions



![overview](resources/overview.png)

A _flow_ defines a sequence of steps, or _stages_ that should be executed on each incoming _event_.  It starts with a _source stage_ and ends with a _sink stage_.  A _flow_ does not actually process any events, it is just a definition.  A _flow_ must be deployed to a cluster of _flow engines_ to actually process events.

A _stage_ is a container for a piece of Python code that is used to process an _event_. The code for a stage is provided in the form of a Python function.  A stage can be stateful or not.  If a stage is stateful, the developer will provide a Python class for the state in addition to a function to process the _event_ (usually a class method).  A _stage_ that only emits events is a _source_.  A _stage_ that only consumes events is a _sink_ , and all other stages are _transform stages_.  

An _event_ is any piece of data, typically one that represents something that happened, like a purchase or a price change.  It can be a `str`, `int`, `dict` or a more complex type.  The are no restrictions on the data type of an _event_, however, the developer of the python code for each _stage_ must know what it will be consuming and what it is expected to produce.  

_Workers_ are the tasks that actually process events.  You can think of a _worker_ as a particular _stage_ executing on a particular _flow engine_.  A _worker_  is responsible for running the user-provided code as a coroutine within the Python [asycio](https://docs.python.org/3/library/asyncio.html) framework.  

A _flow engine_ is a process that is responsible for actually executing the event processing logic.  Each _flow engine_ runs an [asycio](https://docs.python.org/3/library/asyncio.html) main loop.  As such, it is generally single-threaded although some tasks are delegated out to thread pools.  Multiple _flow engines_ should be run on each machine in order to make use of all available CPU.   A cluster of _flow engines_ can span multiple machines as long as there is network connectivity between them.  A _flow engine_ can have _workers_ from multiple _flows_ running at the same time.  The _flow engine_ ensures that all _flows_ continuously make progress by rotating through the deployed _workers_, giving each a turn to use the CPU.

That's enough to get started.   

# Getting Started 

Start a basic 2 node flow cluster: `docker compose up -d`

Next, learn to write a basic flow by studying the  [temperature monitor flow](./examples/temp_monitor_flow.py)

# Known Issues

This project is not ready for production use.  Many issues related to error handling still need to be addressed.  Please see https://github.com/orgs/inferenceflow-ai/projects/1 for a list of issues.  Feel free to report issues or fix them!

# Next

- More examples
- Lots of error handing
- Move "FlowCluster" out of the client.  Give it supervisory functionality.











