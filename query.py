from schemas import Page, PageSchema
import xodb

db = xodb.open('/media/New Volume_/xap/test2', writable=False)
db.map(Page, PageSchema)
