import atexit
import zmq
import xodb
import schemas
from cPickle import loads

db = xodb.open('test_et2', writable=True)
db.map(schemas.Page, schemas.PageSchema)

atexit.register(db.flush)

ctx = zmq.Context()
sink = ctx.socket(zmq.PULL)
sink.setsockopt(zmq.HWM, 1000)
sink.bind('tcp://10.100.0.40:9123')

while True:
    batch = loads(sink.recv())
    for o in batch:
        print o.title
        db.add(o)
