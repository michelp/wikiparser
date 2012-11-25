import zmq
from nltk import batch_ne_chunk, pos_tag, word_tokenize, sent_tokenize
from cPickle import loads, dumps
from operator import itemgetter, methodcaller
from multiprocessing import Process, Queue
from itertools import imap, ifilterfalse
import nilsimsa
import xodb
from xodb.tools import LRUDict


zeroth = itemgetter(0)

nodes =  set(['LOCATION', 'ORGANIZATION', 'PERSON'])


def chunk(text):
    return batch_ne_chunk(
        imap(pos_tag, imap(word_tokenize, sent_tokenize(text))))

def extract_entity_names(t):
    if getattr(t, 'node', None):
        if t.node in nodes:
            yield t.node, ' '.join(imap(zeroth, t))

        else:
            for child in t:
                for n in extract_entity_names(child):
                    yield n

def entities_from(text):
    ents = []
    for t in chunk(text):
        ents.extend(extract_entity_names(t))
    return ents

queue = Queue(100)

def read_source():
    worker_ctx = zmq.Context()
    sink = worker_ctx.socket(zmq.PUSH)
    sink.setsockopt(zmq.HWM, 10)
    sink.connect('tcp://10.100.0.40:9123')

    geonames = xodb.open('names2', writable=False)
    namecache = LRUDict(limit=10000)
    
    while True:
        batch = loads(queue.get())
        results = []
        for o in batch:
            val = (o.text
                   .encode('translit/long')
                   .encode('ascii', 'ignore'))
            print o.title
            if val:
                o.nilsimsa = nilsimsa.Nilsimsa([val]).hexdigest()
                ents = entities_from(val)

            people = []
            locations = []
            orgs = []
            ents2 = []
            ents3 = []
            for t, e in ents:
                sc = e.count(' ')
                if sc == 1:
                    ents2.append(e)
                if sc == 2:
                    ents3.append(e)
                if t == 'PERSON':
                    if namecache.get(e) is not None:
                        locations.append(e)
                    elif geonames.count('name:%s' % e):
                        locations.append(e)
                        namecache[e] = True
                    people.append(e)
                if t == 'LOCATION':
                    if namecache.get(e) is not None:
                        locations.append(e)
                    elif geonames.count('name:%s' % e):
                        locations.append(e)
                        namecache[e] = True
                if t == 'ORGANIZATION':
                    orgs.append(e)

            o.people = people
            o.locations = locations
            o.orgs = orgs
            o.entities = [e[1] for e in ents]
            o.entities2 = ents2
            o.entities3 = ents3
            print o.locations
            results.append(o)
        sink.send(dumps(results))

if __name__ == '__main__':
    pool = [Process(target=read_source) for i in range(24)]
    for p in pool:
        p.daemon = True
        p.start()

    ctx = zmq.Context()
    source = ctx.socket(zmq.PULL)
    source.setsockopt(zmq.HWM, 10)
    source.connect('tcp://10.100.0.40:9124')
    while True:
        queue.put(source.recv())

