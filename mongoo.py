#
# mongo Operations
#
import sys, os
from datetime import datetime
from urlparse import urlparse
import mongoengine as meng
from mongoengine.connection import get_db
from mongoengine import register_connection
from mongoengine.context_managers import switch_db
import config

def connect2db(col, uri):
    parts = urlparse(uri)
    db = os.path.basename(parts[2])
    alias = col._class_name + "_"+ db
    meng.connect(db, alias=alias, host=uri)
    switch_db(col, alias).__enter__()

def mongoo(cb, srccol, srcdb, *args, **kw):
    connect2db(srccol, srcdb)
    
    #naive implementation -- no partitioning or parallelism:
    cur = srccol.objects(**kw) 
    cb(cur, *args)

meng.connect("__neverMIND__", host="127.0.0.1", port=27017)

if __name__ == "__main__":
    import config
    class parsed(meng.Document):
        li_pub_url = meng.StringField()

    def goosrc_cb(q, *args):
        print "goosrc_cb:", q.count(), args

    mongoo(goosrc_cb, parsed, "mongodb://127.0.0.1/local_db", li_pub_url__contains = "www")
    mongoo(goosrc_cb, parsed, "mongodb://tester:%s@127.0.0.1:8501/dev_testdb" % config.password, li_pub_url__contains = "www")
