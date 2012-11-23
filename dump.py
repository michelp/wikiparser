import zmq
import xodb
import schemas
from cPickle import loads

db = xodb.open('test_et', writable=True)
db.map(schemas.Page, schemas.PageSchema)

ctx = zmq.Context()
sink = ctx.socket(zmq.PULL)
sink.setsockopt(zmq.HWM, 10000)
sink.bind('ipc://sink.ipc')

while True:
    o = loads(sink.recv())
    print o.title
    db.add(o)