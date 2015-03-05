#
# mongo Operations
#
import sys, os, time
from enum import Enum
# from datetime import datetime
from urlparse import urlparse
import mongoengine as meng
# from mongoengine.connection import get_db
# from mongoengine import register_connection
from mongoengine.context_managers import switch_db
from mongoengine.context_managers import switch_collection
from extras_mongoengine.fields import StringEnumField

CHUNK = 5
WAITSLEEP = 1.5

PID = os.getpid()
print "pid:", PID

class hkstate(Enum):
    never = 'never'
    working = 'working'
    done = 'done'

class housekeep(meng.Document):
    start = meng.DynamicField()
    end = meng.DynamicField()
    pid = meng.IntField()
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

def mongoo_reset(srccol, destcol, destdb):
    hk_colname = srccol._class_name + '_' + destcol._class_name
    print "dropping", hk_colname
    connect2db(housekeep, destdb)
    switch_collection(housekeep, hk_colname).__enter__()
    housekeep.drop_collection()

def mongoo(cb, key, srccol, srcdb, destcol, destdb, query, **kw):
    if srccol == destcol:
        raise Exception("Source and destination must be different collections")
    connect2db(srccol, srcdb)
    connect2db(destcol, destdb)
    connect2db(housekeep, destdb)
    hk_colname = srccol._class_name + '_' + destcol._class_name
    switch_collection(housekeep, hk_colname).__enter__()
    init = False
    #
    # set up housekeeping if needed
    #
    if housekeep.objects.count() == 0:
        print "initializing housekeeping for", hk_colname
        q = srccol.objects(**query).only(key).order_by(key)
        tot = q.count()
        keys = [x.num for x in q]
        for i in range(0, tot, CHUNK):
            hk = housekeep()
            hk.start = keys[i]
            hk.end = keys[min(i+CHUNK-1, len(keys)-1)]
            hk.save()
        init = True
    else:
        print "skipping initialization, processing"
    #
    # Process what we can
    #
    while housekeep.objects(state = 'never').count():
        hko = housekeep.objects(state = 'never')[0]
        hko.state = 'working'
        hko.pid = PID
        hko.save()
        
        #reload and check if we still own it (avoid race condition)
        hko = housekeep.objects(id = hko.id)[0]
        if hko.pid != PID:
            print "race condition -- waiting, will try later"
            time.sleep(WAITSLEEP)
            continue

        #do stuff            
        query[key + "__gte"] = hko.start
        query[key + "__lte"] = hko.end
        q = srccol.objects(**query)
        kw['init'] = init
        init = False
        cb(q, destcol, **kw)
        hko.state = 'done'
        hko.save()
        print "sleeping"
        time.sleep(WAITSLEEP)
