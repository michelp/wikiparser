import zmq
from nltk import batch_ne_chunk, pos_tag, word_tokenize, sent_tokenize
from cPickle import loads, dumps
from operator import itemgetter
from multiprocessing import Pool
from itertools import imap, ifilterfalse
import nilsimsa


zeroth = itemgetter(0)


def extract_entity_names(t):
    if getattr(t, 'node', None):
        if t.node == 'NE':
            yield ' '.join(imap(zeroth, t))
        else:
            for child in t:
                for n in extract_entity_names(child):
                    yield n

def entities_from(text):
    ents = []
    for t in batch_ne_chunk(imap(pos_tag, 
                                    imap(word_tokenize, sent_tokenize(text))),
                               binary=True):
        ents.extend(extract_entity_names(t))
    ents = list(set(ifilterfalse(lambda e: '_' in e, ents)))
    return ents

worker_ctx = None
sink = None

def read_source(msg):
    global worker_ctx, sink
    if worker_ctx is None:
        worker_ctx = zmq.Context()
        sink = worker_ctx.socket(zmq.PUSH)
        sink.setsockopt(zmq.HWM, 10000)
        sink.connect('tcp://10.100.0.40:9123')

    o = loads(msg)
    val = (o.text
           .encode('translit/long')
           .encode('ascii', 'ignore'))
    print o.title
    if val:
        o.nilsimsa = nilsimsa.Nilsimsa([val]).hexdigest()
        ents = entities_from(val)
        print ents
        o.entities = ents
    sink.send(dumps(o))

if __name__ == '__main__':
    pool = Pool(24)
    ctx = zmq.Context()
    source = ctx.socket(zmq.PULL)
    source.setsockopt(zmq.HWM, 10000)
    source.connect('tcp://10.100.0.40:9124')
    while True:
        messages = []
        for i in xrange(10000):
            messages.append(source.recv())
        pool.map(read_source, messages)

