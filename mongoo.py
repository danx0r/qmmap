#
# mongo Operations
#
import sys, importlib
import pymongo
import mongoengine as meng

# Ooh, it's magic, I know...
caller = importlib.import_module(sys.argv[0][:-3])

class housekeep(meng.Document):
    start = meng.DynamicField(primary_key = True)
    end = meng.DynamicField()
    total = meng.IntField()                             # total # of entries to do
    good = meng.IntField(default = 0)                   # entries successfully processed
    bad = meng.IntField(default = 0)                    # entries we failed to process to completion
    log = meng.ListField()                              # log of misery -- each item a failed processing incident
    state = meng.StringField(default = 'open')
    git = meng.StringField()                                 # git commit of this version of source_destination
    tstart = meng.DateTimeField()  # Time when job started
    time = meng.DateTimeField()  # Time when job finished
    meta = {'indexes': ['state', 'time']}

def mongoo_init(srccol, destcol, key, query, chunk_size):
    connectMongoEngine(destcol)
    if housekeep.objects.count() == 0:
        q = srccol.find(query, [key]).sort([(key, pymongo.ASCENDING)])
        print "initializing %d entries, housekeeping for %s" % (q.count(), housekeep._get_collection_name())
    else:
        raise Exception("no incremental yet")
#         last = housekeep.objects().order_by('-start')[0].end
#         print "last partition field in housekeep:", last
#         query[key + "__gt"] = last
#         q = srccol.objects(**query).only(key).order_by(key)
#         print "added %d entries to %s" % (q.count(), housekeep._get_collection_name())
#         sys.stdout.flush()
    i = 0
    tot = q.limit(chunk_size).count()
    while tot > 0:
        print "housekeeping: %d" % i
        i +=1
        sys.stdout.flush()
        hk = housekeep()
        hk.start = q[0][key]
        hk.end =  q[min(chunk_size-1, tot-1)][key]
        if (hk.start == None or hk.end == None):
            print >> sys.stderr, "ERROR: key field has None. start: %s end: %s" % (hk.start, hk.end)
            raise Exception("key error")
        #calc total for this segment
        qq = {'$and': [query, {key: {'$gte': hk.start}}, {key: {'$lt': hk.end}}]}
        hk.total = srccol.find(qq, [key]).count()
        hk.save()

        #get start of next segment
        qq = {'$and': [query, {key: {'$gt': hk.end}}]}
        q = srccol.find(qq, [key]).sort([(key, pymongo.ASCENDING)])
        tot = q.limit(chunk_size).count()                    #limit count to chunk for speed
    

def process(source_col, 
            dest_col, 
            source_uri="mongodb://127.0.0.1/test", 
            dest_uri="mongodb://127.0.0.1/test",
            source_args={},
            multi=1):
    dbs = pymongo.MongoClient(source_uri).get_default_database()
    dbd = pymongo.MongoClient(dest_uri).get_default_database()
    source = dbs[source_col].find(source_args)
    dest = dbd[dest_col]
    if multi == 1:
        caller.process(source, dest)
    else:
        mongoo_init(dbs[source_col], dbd[dest_col], '_id', source_args, 2)
    
def toMongoEngine(pmobj, metype):
    meobj = metype()
    for key in pmobj:
        if hasattr(meobj, key) and key in pmobj:
            meobj[key] = pmobj[key]
    return meobj

def connectMongoEngine(pmcol):
    if pymongo.version[0] == 2:     #really? REALLY?
        host = pmcol.database.connection.HOST
        port = pmcol.database.connection.PORT
    else:
        host = pmcol.database.client.HOST
        port = pmcol.database.client.PORT
    return meng.connect(pmcol.database.name, host=host, port=port)
