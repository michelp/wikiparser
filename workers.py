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
    for t in batch_ne_chunk(imap(pos_tag, 
                                    imap(word_tokenize, sent_tokenize(text))),
                               ):
        ents.extend(extract_entity_names(t))
    return ents

queue = Queue(100)

def read_source():
    worker_ctx = zmq.Context()
    sink = worker_ctx.socket(zmq.PUSH)
    sink.setsockopt(zmq.HWM, 10)
    sink.connect('tcp://10.100.0.40:9123')

    geonames = xodb.open('names', writable=False)
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
            for t, e in ents:
                if t == 'PERSON':
                    if namecache.get(e) is not None:
                        locations.append(e)
                    elif geonames.count('name:%s' % e):
                        locations.append(e)
                        namecache[e] = True
                    else:
                        people.append(e)
                if t == 'LOCATION':
                    if namecache.get(e) is not None:
                        locations.append(e)
                    elif geonames.count('name:%s' % e):
                        locations.append(e)
                        namecache[e] = True
                    else:
                        locations.append(e)
                if t == 'ORGANIZATION':
                    orgs.append(e)

            o.people = people
            o.locations = locations
            o.orgs = orgs
            print o.people, o.locations, o.orgs
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

