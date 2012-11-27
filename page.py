import re

def page(t):

    t = re.subn('(?s)<!--.*?-->', "", t)[0] # force removing comments
    t = re.subn("(\n\[\[[a-z][a-z][\w-]*:[^:\]]+\]\])+$","", t)[0] # force remove last (=languages) list

    def equal2h(m):
        m = m.groupdict()
        m['level'] = str(len(m['level']))
        return "\n%(title)s\n\n" % m

    t = re.subn("\n(?P<level>=+) *(?P<title>[^\n]*)\\1 *(?=\n)", equal2h, t )[0]

    t = re.sub("'''(.+?)'''", "\\1", t)
    t = re.sub("''(.+?)''", "\\1", t)

    t = re.subn("(?u)^ \t]*==[ \t]*(\w)[ \t]*==[ \t]*\n", '\\1)', t)[0]
    t = re.subn("\[\[([^][|:]*)\]\]", '\\1', t)[0]
    t = re.subn("\[\[([^]|[:]*)\|([^][]*)\]\]", '\\1,\\2', t)[0]
    t = re.subn('\n----', '', t)[0]
    def img2alt(m):
        imgname, other = m.groups()
        alttxt = other[other.rfind('|')+1:]
        return '%s, %s' % (imgname, alttxt)
    t = re.subn("\[\[[Ii]mage:([^.]*)(.*?)\]\]", img2alt, t)[0] # todo: parser l'interieur
    t = re.sub("\n\n+", "\n", t)
    t = re.sub("(<br/>\n?)+", "\n", t) # final cleanup :-)
    return t
