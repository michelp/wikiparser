import re

from xodb import Schema, Text, String, Array, Integer, Boolean


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
        self.image = None
        self.nilsimsa = None

link_re = re.compile(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]')
cat_re = re.compile(r'\[\[Category:(?:[^|\]]*\|)?([^\]]+)\]\]')

def _links(schema, obj, element):
    return filter(lambda l: len(l) > 5 and len(l) < 200, link_re.findall(obj.text))

def _cats(schema, obj, element):
    return filter(None, cat_re.findall(obj.text))

def _size(schema, obj, element):
    return len(obj.text)

def _has_image(schema, obj, element):
    return obj.image is not None


class PageSchema(Schema):

    ignore_invalid_terms = True
    language = 'en'

    title = Text.using(optional=True,
                       prefix=True)

    name = String.using(optional=True,
                        prefix=True,
                        sortable=True,
                        getter=lambda s, o, e: o.title)

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

    links = Array.of(
        Text.using(optional=True,
                   prefix=True,
                   string=True,
                   string_prefix='link')
        ).using(optional=True,
                prefix=False,
                getter=_links)
    
    size = Integer.using(sortable=True,
                         getter=_size)

    num_entities = Integer.using(
        sortable=True,
        getter=lambda s,o,e: len(o.entities))

    num_entities1 = Integer.using(
        sortable=True,
        getter=lambda s,o,e: len(o.entities1))

    num_entities2 = Integer.using(
        sortable=True,
        getter=lambda s,o,e: len(o.entities2))

    num_entities3 = Integer.using(
        sortable=True,
        getter=lambda s,o,e: len(o.entities3))

    num_links = Integer.using(
        sortable=True,
        getter=lambda s,o,e: len(o.link))

    image = String.using(optional=True, sortable=True)

    has_image = Boolean.using(getter=_has_image)

    nilsimsa = String.using(optional=True, sortable=True)
