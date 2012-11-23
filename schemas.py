import re

from xodb import Schema, Text, String, Array, Integer


class Page(object):

    def __init__(self):
        self.title = ''
        self.text = ''
        self.entities = []

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
    entities = Array.of(
        Text.using(optional=True,
                   prefix=True,
                   string=True,
                   string_prefix='entity')
        ).using(optional=True)

    size = Integer.using(sortable=True,
                         getter=_size)