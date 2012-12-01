from pprint import pprint
from collections import OrderedDict
import xodb
from schemas import Page, PageSchema


db = xodb.open('test_et4', writable=False)
db.map(Page, PageSchema)


def _pprint(d, level=0):
    if isinstance(d, OrderedDict):
        for k, v in d.iteritems():
            pad = ' ' * level
            print '%s{' % pad
            print '%s %s: %s,' % (pad, k, _pprint(v, level+4))
            print '%s}' % pad
    else:
        pprint(d)


def e(q, l, count=10, limit=1025):
    _pprint(db.expand(q, [(i, count/(j+1), limit/(j+1), 1, 1) for j, i in enumerate(l)], language='en'))

