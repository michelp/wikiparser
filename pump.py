import zmq
import bz2
import logging
from lxml import etree
from cPickle import dumps

ctx = zmq.Context()
source = ctx.socket(zmq.PUSH)
source.setsockopt(zmq.HWM, 100)
source.bind('tcp://10.100.0.40:9124')

from schemas import Page

log = logging.getLogger(__name__)

BATCH_SIZE = 10

def wikit():
    with bz2.BZ2File('enwiki-latest-pages-articles.xml.bz2') as f:
        current = None
        batch = []
        for event, element in etree.iterparse(f, events=('start', 'end')):
            if event == 'start':
                if element.tag.endswith('page'):
                    current = Page()
                    redirect = False
                if element.tag.endswith('title'):
                    current.title = element.text
                if element.tag.endswith('text'):
                    if element.text:
                        current.text = element.text
                if element.tag.endswith('redirect'):
                    redirect = True
            if event == 'end':
                if element.tag.endswith('page'):
                    if current.text and current.title and not redirect:
                        try:
                            batch.append(current)
                            if len(batch) > BATCH_SIZE:
                                source.send(dumps(batch))
                                batch = []
                        except Exception:
                            log.exception('wtf')
                    element.clear()


wikit()
