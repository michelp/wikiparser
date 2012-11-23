import re

from xodb import Schema, Text, String, Array, Integer, Nilsimsa


class Page(object):

    def __init__(self):
        self.title = ''
        self.text = ''
        self.entities = []
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
    category = Array.of(String.using(optional=True)).using(optional=True,
                                                           facet=True,
                                                           getter=_cats)
    entity = Array.of(String).using(optional=True,
                                    prefix=True)

    size = Integer.using(sortable=True,
                         getter=_size)
    nilsimsa = Nilsimsa.using(from_field='text')
