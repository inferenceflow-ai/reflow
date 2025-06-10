- [ ] Expansion factor is critical to flow control - is there a better way to compute it ?
- [ ] If the queue is remote, there will be a cost to checking it's size.  The performance penalty may be unacceptable. 
      In that case, is there a better way to estimate the downstream capacity ?  Can we send it back from an enqueue
      call and have the upstream estimate ?
- [ ] Support multiple workers in different processes
- [ ] Support multiple workers in different machines
- [ ] Support ordering
- [ ] Implement internal idempotency via event ids and recent event cache
- [ ] Exception handling around all user provided functions (built into *Worker.process most likely).
- [ ] Support re-joining - the inverse of splitting
- [ ] Refine logging to use different loggers for different parts of the program
- [ ] On ordering, suppose we have an ordered source.  We then go through a flat map step that creates multiple
      events.  One notion of ordering would allow the flat mapped events from source event 1 to be processed
      in any order and in parallel but they must all be processed before the events from source event 2.  So
      we have a partial ordering.  s1 < s2 and s1.x < s2.x but there is no ordering among s1.x or s2.x.  How
      can we implement this ?
- [ ] Test: does a single, filtered event ever get acked ?
- [ ] I would like to collapse things like reflow.internal.in_out_map.InOutMap into reflow.internal but last
      time I tried it was a mess.  Revisit.
- [ ] Expose metrics to allow watching the size of the various queues.  The metric should be job specific and
      then worker specific.  It would be nice if there were a task-level rollup. So job/task/worker
- [ ] Error handling in event queues and especially the ZeroMQ event queue server and client
- [x] Need to deal with QueueFull by retrying input events that produced the output events that couldn't be saved.
- [x] Make the event queue test into actual tests (e.g. using pytest, doctest)
- [x] Modify event queue to support a dictionary of offsets.  Only discard entries earlier than the earliest
      acknowledged offset
- [x] I need to move the event counting mechanism into common code
- [x] Think about what, if anything, can be done with sources when all received events can't be saved to the output
      queue. For this case, the source needs to be rewindable.
- [x] Look at worker.py line 52, shouldn't ProducerFn be using the same type vars as those in worker ? Does it ?
- [x] Sources and Sinks are likely to block the event loop and should be offloaded to a separate thread
- [x] Sinks have no way to exert back-pressure
- [x] Figure out how to automate tests like the retry test
- [x] Support jobs that end (probably with sources that end).  Insert a shutdown instruction in the stream.  As
      each processor encounters it, it shuts down. Sub-problem: typically, a group of events will be processed
      and then, as a group, submitted to the output queue.  What happens if, at the end of the "process" method,
      we do not acknowledge all of the events we received.  In that case, we want to keep the worker alive until
      it has a chance to process all events and send them to the downstream queue.  So setting self.finished
      should only happen if all events could be delivered to the downstream queue, including the END_OF_SOURCE
      instruction.  Note that this also doesn't work unless order is preserved, otherwise the shutdown message
      could "pass" another message.
- [x] In SourceWorker, the possibility that the "StopIteration" instruction cannot be enqueued because the
      queue is not available is not being handled.  For rewindable sources, this can be handled by not
      acknowledging the last message until both it and the final instruction have been enqueued.
- [x] Change the back-pressure mechanism: Currently, if the output queue is full, the whole read-from-in
      , process, send to out, ack-in is repeated.  This has the drawback that it will re-run the process
      step because the output queue is full and will also, somewhat unnecessarily, use CPU.  Instead, store the
      output events as internal state in the worker.  The worker simply makes no progress until the output
      events can be enqueued.  If the worker is lost with unacknowledged events then its replacement will
      need to retry the corresponding input events.  Question: if all events aren't received by the out queue,
      which input events do we acknowledge ? Answer: we need the usual input-output event map for this, even
      in sources since they are capable of introducing additional instructions. So the worker will 1. check
      for unacknowledged output events.  2. If there are none, receive and process input, producing output events, and
      output map entries 3. process the output events by sending as many as possible downstream and also acknowledging
      the corresponding upstream events.  Question: when I introduce a END_OF_SOURCE instruction in the output
      queue, there is no corresponding think to acknowledge on the input side  How do I record that in the
      in-out map ? Also, I may want to feed batches of input events to the "process" method.  If I can process
      the whole batch but can't save the resulting output events, which input events do I acknowledge ?
- [x] Common utility for input-output mapping.  Given n outputs were accepted by the downstream queue, how many
      input events do we acknowledge ?  Maybe I could track the information on the event ??   How do I deal with
      filters where an input event may need to be acknowledged even though it produced no output ?
- [x] Change the acknowledge mechanism on queues.  Need to acknowledge an offset, not just a number of events.
      The problem with acknowledging an event count is that it is not idempotent.  It is entirely possible,
      in the event of a failure, that ... wait, maybe its OK.  In what case would I acknowledge the same
      event twice ?  Don't see this as a problem after all. Closing.
- [x] Roll the output event list into the in/out map since they need to be manipulated together.
- [x] Analyze failure case: what if an operation on the queue fails (different from being full) ? What about
      acknowledge ?  Can the attached workers proceed ?  What should they do ?  Is it realistic for the queue
      to have intermittent failures ?
- [x] Source and sink do not currently support filtering.  If this is added, handling of the in_out_map
      needs to be revisited for those workers. - The refactor, its not clear that this is still an issue.
- [x] For processing instructions, they don't create an output event so they don't really factor into back pressure.
      Currently, if there is a list of events followed by a processing instruction, I'm acknowledging the
      input (the one for the processing instruction) when I process the last regular event, as if it were a
      filtered out event.  I should probably not ack it until it is acted upon.  I just have to change the time
      I ack to when I have acted on it (if its a p.i.) or successfully sent it to the out queue if its a
      regular event.
- [x] All mid-stream workers must send processing instructions down stream.
- [x] Refactor and simplify worker so the back pressure algorithm is implemented in only one place.
