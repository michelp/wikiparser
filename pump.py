import zmq
import bz2
import logging
from lxml import etree
from cPickle import dumps

ctx = zmq.Context()
source = ctx.socket(zmq.PUSH)
source.setsockopt(zmq.HWM, 10000)
source.bind('ipc://source.ipc')

from schemas import Page

log = logging.getLogger(__name__)


def wikit():
    with bz2.BZ2File('enwiki-latest-pages-articles.xml.bz2') as f:
        current = None
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
                            source.send(dumps(current))
                        except Exception:
                            log.exception('wtf')
                    element.clear()


wikit()