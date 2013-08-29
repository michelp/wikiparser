import atexit
import logging
import zmq
import xodb
import schemas
from cPickle import loads

db = xodb.open('test_et4', writable=True)
db.map(schemas.Page, schemas.PageSchema)

atexit.register(db.flush)

logging.basicConfig(level=logging.DEBUG)

ctx = zmq.Context()
sink = ctx.socket(zmq.PULL)
sink.setsockopt(zmq.RCVHWM, 1000)
sink.bind('tcp://127.0.0.1:9123')

log = logging.getLogger(__name__)

while True:
    batch = loads(sink.recv())
    for o in batch:
        print o.title
        try:
            db.add(o)
        except Exception:
            log.exception('error')
            
