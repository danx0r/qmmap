#
# mongo Operations
#
import sys, os
from enum import Enum
from datetime import datetime
from urlparse import urlparse
import pymongo
import mongoengine as meng
from mongoengine.connection import get_db
from mongoengine import register_connection
from mongoengine.context_managers import switch_db
from mongoengine.context_managers import switch_collection
from extras_mongoengine.fields import StringEnumField

PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../science") ) 
sys.path.append(PYBASE)
from utils.pp import pp
import config
CHUNK = 5

class hkstate(Enum):
    never = 'never'
    working = 'working'
    done = 'done'

class housekeep(meng.Document):
    start = meng.DynamicField()
    end = meng.DynamicField()
    state = StringEnumField(hkstate, default = 'never')

connect2db_cnt = 0

def connect2db(col, uri):
    global connect2db_cnt, con
    parts = urlparse(uri)
    db = os.path.basename(parts[2])
    if connect2db_cnt:
        alias = col._class_name + "_"+ db
        con = meng.connect(db, alias=alias, host=uri)
        switch_db(col, alias).__enter__()
    else:
        con = meng.connect(db, host=uri)
    connect2db_cnt += 1
    return con

def mongoo(cb, key, srccol, srcdb, destcol, destdb, query, **kw):
    if srccol == destcol:
        raise Exception("Source and destination must be different collections")
    connect2db(srccol, srcdb)
    connect2db(destcol, destdb)
    connect2db(housekeep, destdb)
    switch_collection(housekeep, srccol._class_name + '_' + destcol._class_name).__enter__()
    housekeep.drop_collection()

    """    
    #naive implementation -- no partitioning or parallelism:
    cur = srccol.objects(**query) 
    kw['init'] = True                   #first time only for one thread
    cb(cur.limit(5), destcol, **kw)
    kw['init'] = False
    cb(cur[5:], destcol, **kw)
    """
    
    #chunk it:
    q = srccol.objects(**query).only(key).order_by(key)
    tot = q.count()
    keys = [x.num for x in q]
    init = True
    for i in range(0, tot, CHUNK):
        hk = housekeep()
        hk.start = keys[i]
        hk.end = keys[min(i+CHUNK-1, len(keys)-1)]
        hk.save()
        while housekeep.objects(state = 'never').count():
            hko = housekeep.objects(state = 'never')[0]
            query[key + "__gte"] = hko.start
            query[key + "__lte"] = hko.end
            q = srccol.objects(**query)
            kw['init'] = init
            init = False
            cb(q, destcol, **kw)
            hko.state = 'done'
            hko.save()
    
# meng.connect("__neverMIND__", host="127.0.0.1", port=27017)

if __name__ == "__main__":
    import config

    class goosrc(meng.Document):
        num = meng.IntField(primary_key = True)

    class goodest(meng.Document):
        numnum = meng.StringField()

    connect2db(goosrc, "mongodb://127.0.0.1/local_db")
    goosrc.drop_collection()

    for i in range(0, 50, 3):
        g = goosrc(num = i)
        g.save()

    def goosrc_cb(src, dest, init = False, reset = False):
        print "goosrc_cb: source count:", src.count(), "init:", init, "reset:", reset
        if init and reset:
            dest.drop_collection()
            print "init: dropped", dest
        for x in src:
            if x.num != 15:
                print "goosrc_cb process:", x.num
                d = dest()
                d.numnum = str(x.num*2)
                d.save()
            else:
                print "goosrc_cb: skipping", x.num

    mongoo( goosrc_cb,
            'num',                                                              #key field for partitioning 
            goosrc,                                                             #source collection
            "mongodb://127.0.0.1/local_db",                                     #source host/database
            goodest,                                                            #destination collection
#             "mongodb://tester:%s@127.0.0.1:8501/dev_testdb" % config.password,  #destination host/database
            "mongodb://127.0.0.1/local_db",
            {"num__ne": 3},                                                    #query params for source (mongoengine)
            reset = True                                                        #extra parameters for callback
        )

    print "goodest:"
    pp(goodest.objects)