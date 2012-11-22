import zmq
import re
import nltk
from cPickle import loads, dumps
from operator import itemgetter
from multiprocessing import Pool
import string
import nilsimsa


zeroth = itemgetter(0)
regex = re.compile('^.*[%s].*$' % re.escape(string.punctuation))


def extract_entity_names(t):
    entity_names = []

    if getattr(t, 'node', None):
        if t.node == 'NE':
            entity_names.append(' '.join([child[0] for child in t]))
        else:
            for child in t:
                entity_names.extend(extract_entity_names(child))

    return entity_names

def _entities_from(text):
    ents = []
    tokenized_sentences = map(nltk.word_tokenize, nltk.sent_tokenize(text))
    tagged_sentences = map(nltk.pos_tag, tokenized_sentences)
    chunked_sentences = nltk.batch_ne_chunk(tagged_sentences, binary=True)
    for tree in chunked_sentences:
        ents.extend(extract_entity_names(tree))
    return ents

worker_ctx = None

def read_source(msg):
    global worker_ctx, sink
    if worker_ctx is None:
        worker_ctx = zmq.Context()
        sink = worker_ctx.socket(zmq.PUSH)
        sink.setsockopt(zmq.HWM, 1000)
        sink.connect('ipc://sink.ipc')

    o = loads(msg)
    val = (o.text
           .encode('translit/long')
           .encode('ascii', 'ignore'))
    if val:
        o.nilsimsa = nilsimsa.Nilsimsa([val]).hexdigest()
        ents = _entities_from(val)
        print ents
        o.entities = ents
    sink.send(dumps(o))

if __name__ == '__main__':
    pool = Pool(8, maxtasksperchild=100)
    ctx = zmq.Context()
    source = ctx.socket(zmq.PULL)
    source.setsockopt(zmq.HWM, 1000)
    source.connect('ipc://source.ipc')
    while True:
        messages = []
        for i in xrange(1000):
            messages.append(source.recv())
        pool.map(read_source, messages)

