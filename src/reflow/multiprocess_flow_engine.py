# Create a worker for each stage, for each core.  Connect the workers on the same core by local
# queues and those on different cores with remote queues. The remote queue will still be local to the
# queue consumer (which calls get_events and acknowledge) but remote to the queue writer (that calls enqueue),

# Wait, that makes no sense.  Each worker has only one input queue, but local workers can still access it
# locally.  So, if I start with a local set of workers on each core.  Each is just an event loop running workers.
# Deploying a job is just sending the graph.  What do do about Python dependencies ?  For now, I'll use the idea
# of an "environment".  Jobs can be deployed to environments, probably using poetry.
#
# Each machine has an agent that exposes an control interface via REST. Functions include:
# New engine - create a local engine which runs an event loop and just sits and runs workers and exposes monitoring endpoints
#    The agent stores state about what is present on each, etc.  RocksDB ? Mongo ? Just a file ?
# Need this, but could just manually run engines for now.
#
# To deploy a job across multiple engines.
# - deploy it to each engine
# - for each worker, for each server / thread
#   - wrap it's input queue in RPC (unless it's a source)
#   - connect the upstream from each other server to this
#
# Each worker, instead of having an output queue can have a routing proxy.  Initial routing options should
# include round-robin, local only and key based.
#

class MultiprocessingJobBuilder:
    pass
