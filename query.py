from pprint import pprint
from collections import OrderedDict
import xodb
from schemas import Page, PageSchema
import json


db = xodb.open('test_et4', writable=False)
db.map(Page, PageSchema)


def e(q, l, count=10, limit=1025):
    print json.dumps(db.expand(q, [(i, count, limit, 1, 1) for j, i in enumerate(l)], language='en'), indent=4)


def q(terms, limit=20):
    # is it an entity?
    terms = " ".join(terms.strip().lower().split())
    entity_term = 'entity:"%s"' % terms
    if db.estimate(entity_term) > 20:
        for r in db.query(entity_term, language='en', limit=limit):
            print r.title
