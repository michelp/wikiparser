import re
import zmq
from nltk import batch_ne_chunk, pos_tag, word_tokenize, sent_tokenize
from cPickle import loads, dumps
from operator import itemgetter, methodcaller
from multiprocessing import Process, Queue
from itertools import imap, ifilterfalse
import nilsimsa
import xodb
from xodb.tools import LRUDict
import page

zeroth = itemgetter(0)

nodes =  set(['NE', 'LOCATION', 'ORGANIZATION', 'PERSON'])

image_re = re.compile(r'\|\s*image\s*=\s*(.*)$', re.MULTILINE)


def chunk(text, binary=True):
    return batch_ne_chunk(
        imap(pos_tag, imap(word_tokenize, sent_tokenize(text))),
        binary=binary)

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
    sink.setsockopt(zmq.SNDHWM, 10)
    sink.connect('tcp://127.0.0.1:9123')

    while True:
        batch = loads(queue.get())
        results = []
        for o in batch:
            search = image_re.search(o.text)
            if search is not None:
                image = search.group(1).strip()
                if '.jpg' in image:
                    image = image.split('.jpg')[0] + '.jpg'
                    if ':' in image:
                        image = image.split(':')[1]
                    o.image = image
                    print image
                elif '.svg' in image:
                    image = image.split('.svg')[0] + '.svg'
                    if ':' in image:
                        image = image.split(':')[1]
                    o.image = image
                    print image
                elif '.png' in image:
                    image = image.split('.png')[0] + '.png'
                    if ':' in image:
                        image = image.split(':')[1]
                    o.image = image
                    print image

            print o.title

            val = (o.text
                   .encode('translit/long')
                   .encode('ascii', 'ignore'))
            if val:
                val = page.page(val)
                o.nilsimsa = nilsimsa.Nilsimsa([val]).hexdigest()
                ents = entities_from(val)

            ents0 = []
            ents1 = []
            ents2 = []
            ents3 = []
            for t, e in ents:
                sc = e.count(' ')
                ents0.append(e)
                if sc == 0:
                    ents1.append(e)
                if sc == 1:
                    ents2.append(e)
                if sc == 2:
                    ents3.append(e)

            o.entities = ents0
            o.entities1 = ents1
            o.entities2 = ents2
            o.entities3 = ents3
            results.append(o)
        sink.send(dumps(results))

if __name__ == '__main__':
    pool = [Process(target=read_source) for i in range(8)]
    for p in pool:
        p.daemon = True
        p.start()

    ctx = zmq.Context()
    source = ctx.socket(zmq.PULL)
    source.setsockopt(zmq.RCVHWM, 10)
    source.connect('tcp://127.0.0.1:9124')
    while True:
        queue.put(source.recv())

