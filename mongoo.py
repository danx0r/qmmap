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

connect2db_cnt = 0

def connect2db(col, uri):
    global connect2db_cnt
    parts = urlparse(uri)
    db = os.path.basename(parts[2])
    if connect2db_cnt:
        alias = col._class_name + "_"+ db
        meng.connect(db, alias=alias, host=uri)
        switch_db(col, alias).__enter__()
    else:
        meng.connect(db, host=uri)
    connect2db_cnt += 1

def mongoo(cb, srccol, srcdb, destcol, destdb, *args, **kw):
    if srccol == destcol:
        raise Exception("Source and destination must be different collections")
    connect2db(srccol, srcdb)
    connect2db(destcol, destdb)
    
    #naive implementation -- no partitioning or parallelism:
    cur = srccol.objects(**kw) 
    cb(cur, destcol, *args)

# meng.connect("__neverMIND__", host="127.0.0.1", port=27017)

if __name__ == "__main__":
    import config
    class parsed(meng.Document):
        li_pub_url = meng.StringField()
    class li_profiles(meng.Document):
        pass

    def goosrc_cb(src, dest):
        print "goosrc_cb:", src.count(), dest.objects.count()

    mongoo( goosrc_cb, 
            parsed,                                                             #source collection
            "mongodb://127.0.0.1/local_db",                                     #source host/database
            li_profiles,                                                        #destination collection
            "mongodb://tester:%s@127.0.0.1:8501/dev_testdb" % config.password,  #destination host/database
            li_pub_url__contains = "www"                                        #source query filter terms
        )
