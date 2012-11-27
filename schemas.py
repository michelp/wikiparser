import re

from xodb import Schema, Text, String, Array, Integer, Nilsimsa


class Page(object):

    def __init__(self):
        self.title = ''
        self.text = ''
        self.category = []
        self.people = []
        self.orgs = []
        self.locations = []
        self.entities = []
        self.entities2 = []
        self.entities3 = []
        self.link = []
        self.nilsimsa = ''

link_re = re.compile(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]')
cat_re = re.compile(r'\[\[Category:(?:[^|\]]*\|)?([^\]]+)\]\]')

def _links(schema, obj, element):
    return filter(lambda l: len(l) > 5 and len(l) < 200, link_re.findall(obj.text))

def _cats(schema, obj, element):
    return filter(None, cat_re.findall(obj.text))

def _size(schema, obj, element):
    return len(obj.text)


class PageSchema(Schema):

    ignore_invalid_terms = True
    language = 'en'

    title = Text.using(optional=True,
                       prefix=True,
                       string=True,
                       string_prefix='name')

    text = Text.using(prefix=False, optional=True)

    category = Array.of(
        String.using(optional=True)
        ).using(optional=True,
                facet=True,
                getter=_cats)

    entities = Array.of(
        Text.using(optional=True,
                   prefix=True,
                   string=True,
                   string_prefix='entity',
                   )).using(optional=True, prefix=False)

    entities1 = Array.of(
        Text.using(optional=True,
                   prefix=True,
                   string=True,
                   string_prefix='entity1',
                   )).using(optional=True, prefix=False)

    entities2 = Array.of(
        Text.using(optional=True,
                   prefix=True,
                   string=True,
                   string_prefix='entity2',
                   )).using(optional=True, prefix=False)

    entities3 = Array.of(
        Text.using(optional=True,
                   prefix=True,
                   string=True,
                   string_prefix='entity3',
                   )).using(optional=True, prefix=False)

    link = Array.of(
        String.using(optional=True)
        ).using(optional=True,
                prefix=True,
                getter=_links)
    
    size = Integer.using(optional=True,
                         sortable=True,
                         getter=_size)

    nilsimsa = String.using(optional=True, sortable=True)
