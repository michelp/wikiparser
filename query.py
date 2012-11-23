from schemas import Page, PageSchema
import xodb

db = xodb.open('test_et', writable=False)
db.map(Page, PageSchema)
