import asyncio
from asyncio import QueueEmpty
from typing import Callable, Any, List, TypeVar, Self, Optional

hamlet = """
To be, or not to be, that is the question:
Whether 'tis nobler in the mind to suffer
The slings and arrows of outrageous fortune,
Or to take arms against a sea of troubles
And by opposing end them. To die—to sleep,
No more; and by a sleep to say we end
The heart-ache and the thousand natural shocks
That flesh is heir to: 'tis a consummation
Devoutly to be wish'd. To die, to sleep;
To sleep, perchance to dream—ay, there's the rub:
For in that sleep of death what dreams may come,
When we have shuffled off this mortal coil,
Must give us pause—there's the respect
That makes calamity of so long life.
For who would bear the whips and scorns of time,
Th'oppressor's wrong, the proud man's contumely,
The pangs of dispriz'd love, the law's delay,
The insolence of office, and the spurns
That patient merit of th'unworthy takes,
When he himself might his quietus make
With a bare bodkin? Who would fardels bear,
To grunt and sweat under a weary life,
But that the dread of something after death,
The undiscovere'd country, from whose bourn
No traveller returns, puzzles the will,
And makes us rather bear those ills we have
Than fly to others that we know not of?
Thus conscience doth make cowards of us all,
And thus the native hue of resolution
Is sicklied o'er with the pale cast of thought,
And enterprises of great pith and moment
With this regard their currents turn awry
And lose the name of action"""


# let's count the words using stream processing
#
# the stages will be
#   split - the input is a sentence and the output is words
#   clean - here we will normalize and remove stop words
#   count by unique word - since this requires state, we have to assign specific processors to specific words
#
#  We will assign 2 task instances to each stage except the initial split stage.  Note that tasks instances
#  are dedicated to executing a particular stage of the stream and may have state.  Each task instance has
#  an associated inbox and it processes items from the inbox one at a time.  Tasks are not pinned to a specific
#  CPU or worker thread.  They may even be able to move between machines (we'll see).  A scheduling algorithm
#  is used to decide which task is run next.  However, task instances do limit the maximum possible concurrency
#  of a stage.
#
#  As mentioned, each task instance has an associated input queue, excepts sources, which get their
#  input from an external system.  The result of processing an event by a task will be delivered to 0 or more
#  input queues for subsequent inboxes according to the DAG.  A task may also emit events to an external system
#  or not at all.  A task that doesn't emit events to subsequent inboxes is a sink.
#
#  Initially we are assuming that actually processing an event (i.e. the "transformer" method) never blocks.
#  Later, we'll introduce a mechanism to handle that

IN = TypeVar('IN')
OUT = TypeVar('OUT')

class Task:
    def __init__(self,
                 transformer: Callable[[List[IN]], Optional[List[OUT]]],
                 next_tasks: List[List[Self]] = None,
                 routing_key_extractor: Callable[[OUT], int] = None,
                 flatten: bool = False):
        # Note: we are using typing.Self which means this requires 3.11+
        self.transformer = transformer
        self.input_queue = asyncio.Queue(maxsize=1000)
        self.next_tasks = next_tasks
        self.routing_key_extractor = routing_key_extractor
        self.flatten = flatten

    async def enqueue_events(self, events: List[Any])->None:
        for event in events:
            await self.input_queue.put(event)

    async def process(self, max_event_count:int):
        ready_events = []
        try:
            for _ in range(max_event_count):
                ready_events.append(self.input_queue.get_nowait())
        except QueueEmpty:
            pass

        if len(ready_events) == 0:
            return          # RETURN if there are no events to process

        results = self.transformer(ready_events)
        if self.flatten:
            results = [item for sublist in results for item in sublist]

        if not self.next_tasks:
            return

        for task_group in self.next_tasks:
            i = 0
            n = len(task_group)
            task_lists = [[] for _ in task_group]
            for result in results:
                if self.routing_key_extractor:
                    s = hash(self.routing_key_extractor(result)) % n
                else:
                    i += 1
                    s = i % n

                task_lists[s].append(result)

            for t, task in enumerate(task_group):
                await task.enqueue_events(task_lists[t])

# for stateful transformers, we can use a method on a class
# the class instance encapsulates the state
class WordCounter:
    def __init__(self):
        self.counts = {}

    def count_words(self, words: List[str]):
        for word in words:
            self.count_word(word)

    def count_word(self, w: str):
        if w not in self.counts:
            self.counts[w] = 1
        else:
            c = self.counts[w]
            self.counts[w] = c + 1

    def print_counts(self):
        for word, count in self.counts.items():
            print(f'{word}:{count}')

# for stateless transformers, we can just use functions
def clean(words: List[str])->List[str]:
    stopwords = ['the','a','an','of','to','in','and','or','be', 'is','his','her']
    cleaned = [w.strip(',.?') for w in words]
    lc_words = [w.lower() for w in cleaned]
    return [w for w in lc_words if w not in stopwords]


# Let's build this from end to beginning
tasks = []

wc1 = WordCounter()
wc2 = WordCounter()

wordcount_tasks = [ Task(wc1.count_words), Task(wc2.count_words)]

# clean tasks send output to wordcount tasks according to the provided key extractor
clean_tasks = [ Task(clean, [wordcount_tasks], lambda w: hash(w)) for _ in range(2)]

split_task = Task(lambda text_array: [t.split() for t in text_array], [clean_tasks], flatten=True)

async def main():
    await split_task.enqueue_events([hamlet, "When pigs fly", "mercury venus earth mars jupiter saturn uranus neptune"])
    for _ in range(100):
        await(split_task.process(100))
        for clean_task in clean_tasks:
            await clean_task.process(100)

        for wordcount_task in wordcount_tasks:
            await wordcount_task.process(100)

    print('-----------------------')
    wc1.print_counts()
    print('-----------------------')
    wc2.print_counts()
    print('-----------------------')

asyncio.run(main())

