import sys
import zmq
import bz2
import logging
import xodb
from xodb.database import Record
from lxml import etree
from cPickle import dumps

ctx = zmq.Context()
source = ctx.socket(zmq.PUSH)
source.setsockopt(zmq.SNDHWM, 100)
source.bind('tcp://127.0.0.1:9124')

from schemas import Page

log = logging.getLogger(__name__)

BATCH_SIZE = 10

def wikit():
    num = 0
    if len(sys.argv) > 1:
        db = xodb.open('test_et4', writable=False)
        last = db.backend.get_doccount()
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
                    # num += 1
                    # if num < last:
                    #     print 'Skipping  ', num, " ", last, " ", current.title
                    #     element.clear()
                    #     continue
                    if current.text and current.title and not redirect:
                        if not current.title.startswith(('Template:', 'Category:', 'File:')):
                            print "Pumping ", current.title
                            try:
                                batch.append(current)
                                if len(batch) > BATCH_SIZE:
                                    source.send(dumps(batch))
                                    batch = []
                            except Exception:
                                log.exception('wtf')
                    element.clear()


wikit()
