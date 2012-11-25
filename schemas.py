import re

from xodb import Schema, Text, String, Array, Integer, Nilsimsa


class Page(object):

    def __init__(self):
        self.title = ''
        self.text = ''
        self.people = []
        self.orgs = []
        self.locations = []
        self.link = []
        self.nilsimsa = ''

link_re = re.compile(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]')
cat_re = re.compile(r'\[\[Category:(?:[^|\]]*\|)?([^\]]+)\]\]')

def _links(schema, obj, element):
    return filter(None, link_re.findall(obj.text))

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

    text = Text.using(prefix=False)

    category = Array.of(
        String.using(optional=True)
        ).using(optional=True,
                facet=True,
                getter=_cats)

    people = Array.of(
        Text.using(optional=True,
                   prefix=True,
                   string=True,
                   string_prefix='person',
                   )).using(optional=True, prefix=False)

    orgs = Array.of(
        Text.using(optional=True,
                   prefix=True,
                   string=True,
                   string_prefix='org',
                   )).using(optional=True, prefix=False)

    locations = Array.of(
        Text.using(optional=True,
                   prefix=True,
                   string=True,
                   string_prefix='location',
                   )).using(optional=True, prefix=False)


    link = Array.of(String).using(optional=True,
                                  prefix=True)

    size = Integer.using(sortable=True,
                         getter=_size)

    nilsimsa = String.using(optional=True, sortable=True)
