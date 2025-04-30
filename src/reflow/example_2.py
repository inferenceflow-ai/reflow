from typing import Callable, List, TypeVar, Optional, Generic, Dict

from sources import TestSource

# In this example, I want to separate out the job definition from the deployment.
#
# Other steps will be to introduce end-of-job markers and also
# enable finite jobs to be serviced rapidly by leaving the constituent task in place
#
# Finally, add an aggregation step.

hamlet_sentences =  [
"""To be, or not to be, that is the question:
Whether 'tis nobler in the mind to suffer
The slings and arrows of outrageous fortune,
Or to take arms against a sea of troubles
And by opposing end them.""",
"""To die—to sleep,
No more; and by a sleep to say we end
The heart-ache and the thousand natural shocks
That flesh is heir to: 'tis a consummation
Devoutly to be wish'd. """,
"""To die, to sleep;
To sleep, perchance to dream—ay, there's the rub:
For in that sleep of death what dreams may come,
When we have shuffled off this mortal coil,
Must give us pause—there's the respect
That makes calamity of so long life.""",
"""For who would bear the whips and scorns of time,
Th'oppressor's wrong, the proud man's contumely,
The pangs of dispriz'd love, the law's delay,
The insolence of office, and the spurns
That patient merit of th'unworthy takes,
When he himself might his quietus make
With a bare bodkin?""",
"""Who would fardels bear,
To grunt and sweat under a weary life,
But that the dread of something after death,
The undiscovere'd country, from whose bourn
No traveller returns, puzzles the will,
And makes us rather bear those ills we have
Than fly to others that we know not of?""",
"""Thus conscience doth make cowards of us all,
And thus the native hue of resolution
Is sicklied o'er with the pale cast of thought,
And enterprises of great pith and moment
With this regard their currents turn awry
And lose the name of action"""]

IN_EVENT_TYPE = TypeVar('IN_EVENT_TYPE')
OUT_EVENT_TYPE = TypeVar('OUT_EVENT_TYPE')
STATE_TYPE = TypeVar('STATE_TYPE')

class FlowStage:
    def __init__(self):
        self.next_stages = []

    def then_do(self, transform: "TransformStage")->"TransformStage":
        self.next_stages.append(transform)
        return transform

    def write_to(self, sink: "Sink") -> None:
        self.next_stages.append(sink)


class SourceStage(FlowStage):
    def __init__(self):
        FlowStage.__init__(self)

class StatelessSourceStage(Generic[OUT_EVENT_TYPE], SourceStage):
    def __init__(self,  data_fn: Callable[[int], OUT_EVENT_TYPE]):
        SourceStage.__init__(self)
        self.data_fn = data_fn


class StatefulSourceStage(Generic[OUT_EVENT_TYPE, STATE_TYPE], SourceStage):
    def __init__(self, init_fn: Callable[..., OUT_EVENT_TYPE], data_fn: Callable[[STATE_TYPE, int], OUT_EVENT_TYPE]):
        SourceStage.__init__(self)
        self.init_fn = init_fn
        self.data_fn = data_fn


class TransformStage(FlowStage):
    def __init__(self):
        FlowStage.__init__(self)

class StatelessTransformStage(Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE], TransformStage):
    def __init__(self, transform_fn: Callable[[List[IN_EVENT_TYPE]], List[OUT_EVENT_TYPE]]):
        TransformStage.__init__(self)
        self.transform_fn = transform_fn

    @classmethod
    def from_fn(cls, transform_fn: Callable[[IN_EVENT_TYPE], Optional[OUT_EVENT_TYPE]]):
        batch_fn = lambda in_event_list: [transform_fn(e) for e in in_event_list]
        return cls(batch_fn)


class StatefulTransformStage(Generic[STATE_TYPE, IN_EVENT_TYPE, OUT_EVENT_TYPE], TransformStage):
    def __init__(self,
                 init_fn: Callable[[...], STATE_TYPE],
                 transform_fn: Callable[[STATE_TYPE, List[IN_EVENT_TYPE]], List[OUT_EVENT_TYPE]]):
        TransformStage.__init__(self)
        self.init_fn = init_fn
        self.transform_fn = transform_fn

    @classmethod
    def from_fns(cls,
                init_fn: Callable[[...], STATE_TYPE],
                transform_fn: Callable[[STATE_TYPE, IN_EVENT_TYPE], OUT_EVENT_TYPE]):
        batch_fn = lambda s, in_event_list: [transform_fn(s,e) for e  in in_event_list]
        return cls(init_fn, batch_fn)


class StatelessFlattenStage(Generic[IN_EVENT_TYPE, OUT_EVENT_TYPE], TransformStage):
    def __init__(self, transform_fn: Callable[[IN_EVENT_TYPE], List[OUT_EVENT_TYPE]]):
        TransformStage.__init__(self)
        self.transform_fn = transform_fn


class StatefulFlattenStage(Generic[STATE_TYPE, IN_EVENT_TYPE, OUT_EVENT_TYPE], TransformStage):
    def __init__(self,
                 init_fn: Callable[[...], STATE_TYPE],
                 transform_fn: Callable[[STATE_TYPE, IN_EVENT_TYPE], List[OUT_EVENT_TYPE]]):
        TransformStage.__init__(self)
        self.init_fn = init_fn
        self.transform_fn = transform_fn



class StatefulAggregationStage(Generic[STATE_TYPE, IN_EVENT_TYPE, OUT_EVENT_TYPE], TransformStage):
    def __init__(self,
                 init_fn: Callable[...,STATE_TYPE],
                 accumulate_fn: Callable[[STATE_TYPE, IN_EVENT_TYPE], None],
                 finalize_fn: Callable[[STATE_TYPE], OUT_EVENT_TYPE]):
        TransformStage.__init__(self)
        self.init_fn = init_fn
        self.accumulate_fn = accumulate_fn
        self.finalize_fn = finalize_fn


class Sink:
    def __init__(self):
        pass

class StatelessSink(Generic[IN_EVENT_TYPE], Sink):
    def __init__(self, consumer_fn: Callable[[List[IN_EVENT_TYPE]], None]):
        Sink.__init__(self)
        self.consumer_fn = consumer_fn


class StatefulSink(Generic[STATE_TYPE, IN_EVENT_TYPE], Sink):
    def __init__(self,
                 init_fn: Callable[[...], STATE_TYPE],
                 consumer_fn: Callable[[STATE_TYPE, List[IN_EVENT_TYPE]], None]):
        Sink.__init__(self)
        self.init_fn = init_fn
        self.consumer_fn = consumer_fn



class Flow:
    def __init__(self):
        self.sources = []

    def read_from(self, source: SourceStage)->FlowStage:
        self.sources.append(source)
        return source





test_source_def = StatefulSourceStage(TestSource, lambda test_source, n: test_source.poll(n))

split_def = StatelessFlattenStage(lambda s: s.split())

def clean(word:str)->Optional[str]:
    stopwords = ['the','a','an','of','to','in','and','or','be', 'is','his','her']
    cleaned = word.strip(',.?')
    lowered = cleaned.lower()
    if lowered not in stopwords:
        return lowered
    else:
        return None

clean_def = StatelessTransformStage.from_fn(clean)

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

    def get_counts(self):
        return self.counts


count_def = StatefulAggregationStage[WordCounter, str, Dict[str,str]](
    WordCounter,
    lambda wc, word: wc.count_word(),
    lambda wc: wc.get_counts())

def print_counts(count_list: List[Dict[str, int]]):
    for count in count_list:
        print(count)
        print()

sink_def = StatelessSink(print_counts)

flow = Flow()
flow.read_from(test_source_def).then_do(split_def).then_do(clean_def).then_do(count_def).write_to(sink_def)

