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
PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../science") ) 
sys.path.append(PYBASE)
from utils.pp import pp
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

def mongoo(cb, srccol, srcdb, destcol, destdb, query, **kw):
    if srccol == destcol:
        raise Exception("Source and destination must be different collections")
    connect2db(srccol, srcdb)
    connect2db(destcol, destdb)
    
    #naive implementation -- no partitioning or parallelism:
    cur = srccol.objects(**query) 
    kw['init'] = True                   #first time only for one thread
    cb(cur.limit(1), destcol, **kw)
    kw['init'] = False
    cb(cur[1:], destcol, **kw)

# meng.connect("__neverMIND__", host="127.0.0.1", port=27017)

if __name__ == "__main__":
    import config

    class goosrc(meng.Document):
        num = meng.IntField(primary_key = True)

    class goodest(meng.Document):
        numnum = meng.StringField()

    connect2db(goosrc, "mongodb://127.0.0.1/local_db")

    for i in range(3):
        g = goosrc(num = i)
        g.save()

    def goosrc_cb(src, dest, init = False, reset = False):
        print "goosrc_cb:", src.count(), dest.objects.count(), init, reset
        if init and reset:
            dest.drop_collection()
            print "dropped", dest
        for x in src:
            print "goosrc_cb process:", x.num
            d = dest()
            d.numnum = str(x.num*2)
            d.save()

    mongoo( goosrc_cb, 
            goosrc,                                                             #source collection
            "mongodb://127.0.0.1/local_db",                                     #source host/database
            goodest,                                                            #destination collection
#             "mongodb://tester:%s@127.0.0.1:8501/dev_testdb" % config.password,  #destination host/database
            "mongodb://127.0.0.1/local_db",
            {"num__gte": 1},                                                    #query params for source (mongoengine)
            reset = True                                                        #extra parameters for callback
        )

    print "goodest:"
    pp(goodest.objects)