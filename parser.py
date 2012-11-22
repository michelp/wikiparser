import bz2
import xodb
import logging
from lxml import etree

from schemas import Page, PageSchema


log = logging.getLogger(__name__)
db = xodb.open('/home/michel/xap/ms_test2')
db.map(Page, PageSchema)


redirects = {}


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
                        db.add(current)
                    except Exception:
                        log.exception('wtf')
                element.clear()
