#
# mongo Operations
#
import sys, os, importlib, datetime, time, traceback
import pymongo
import mongoengine as meng
from mongoengine.context_managers import switch_collection

NULL = open(os.devnull, "w")

class housekeep(meng.Document):
    start = meng.DynamicField(primary_key = True)
    end = meng.DynamicField()
    total = meng.IntField()                             # total # of entries to do
    good = meng.IntField(default = 0)                   # entries successfully processed
#     bad = meng.IntField(default = 0)                    # entries we failed to process to completion
#     log = meng.ListField()                              # log of misery -- each item a failed processing incident
    state = meng.StringField(default = 'open')
#     git = meng.StringField()                                 # git commit of this version of source_destination
    tstart = meng.DateTimeField()  # Time when job started
    time = meng.DateTimeField()  # Time when job finished
    meta = {'indexes': ['state', 'time']}

def _init(srccol, destcol, key, query, chunk_size, verbose):
    connectMongoEngine(destcol)
    hk_colname = srccol.name + '_' + destcol.name
    switch_collection(housekeep, hk_colname).__enter__()
    housekeep.drop_collection()
    q = srccol.find(query, [key]).sort([(key, pymongo.ASCENDING)])
    if verbose & 2: print "initializing %d entries, housekeeping for %s" % (q.count(), housekeep._get_collection_name())
#     else:
#         raise Exception("no incremental yet")
# #         last = housekeep.objects().order_by('-start')[0].end
# #         if verbose & 2: print "last partition field in housekeep:", last
# #         query[key + "__gt"] = last
# #         q = srccol.objects(**query).only(key).order_by(key)
# #         if verbose & 2: print "added %d entries to %s" % (q.count(), housekeep._get_collection_name())
# #         sys.stdout.flush()
    i = 0
    tot = q.limit(chunk_size).count()
    while tot > 0:
        if verbose & 2: print "housekeeping: %d" % i
        i +=1
        sys.stdout.flush()
        hk = housekeep()
        hk.start = q[0][key]
        hk.end =  q[min(chunk_size-1, tot-1)][key]
        if (hk.start == None or hk.end == None):
            if verbose & 2: print >> sys.stderr, "ERROR: key field has None. start: %s end: %s" % (hk.start, hk.end)
            raise Exception("key error")
        #calc total for this segment
        qq = {'$and': [query, {key: {'$gte': hk.start}}, {key: {'$lte': hk.end}}]}
        hk.total = srccol.find(qq, [key]).count()
        hk.save()

        #get start of next segment
        qq = {'$and': [query, {key: {'$gt': hk.end}}]}
        q = srccol.find(qq, [key]).sort([(key, pymongo.ASCENDING)])
        tot = q.limit(chunk_size).count()                    #limit count to chunk for speed

def _process(init, proc, src, dest):
    if init:
        print "INIT"
        try:
            init(src, dest)
        except:
            print >> sys.stderr, "***EXCEPTION (process)***"
            print >> sys.stderr, traceback.format_exc()
            print >> sys.stderr, "***END EXCEPTION***"
            return 0
    else:
        print "NINI"
    good = 0
    for doc in src:
        try:
            ret = proc(doc)
            if ret != None:
                dest.save(ret)
            good += 1
        except:
            print >> sys.stderr, "***EXCEPTION (process)***"
            print >> sys.stderr, traceback.format_exc()
            print >> sys.stderr, "***END EXCEPTION***"
    return good

def _do_chunks(init, proc, src_col, dest_col, query, key, verbose):
    while housekeep.objects(state = 'open').count():
        tnow = datetime.datetime.utcnow()
        raw = housekeep._collection.find_and_modify(
            {'state': 'open'},
            {
                '$set': {
                    'state': 'working',
                    'tstart': tnow,
                }
            }
        )
        #if raw==None, someone scooped us
        if raw != None:
            #reload as mongoengine object -- _id is .start (because set as primary_key)
            hko = housekeep.objects(start = raw['_id'])[0]
            # Record git commit for sanity
#             hko.git = git.Git('.').rev_parse('HEAD')
#             hko.save()
            # get data pointed to by housekeep
            qq = {'$and': [query, {key: {'$gte': hko.start}}, {key: {'$lte': hko.end}}]}
            cursor = src_col.find(qq)
            if verbose & 2: print "mongo_process: %d elements in chunk %s-%s" % (cursor.count(), hko.start, hko.end)
            sys.stdout.flush()
            if not verbose & 1:
                print "OFF"
                oldstdout = sys.stdout
                sys.stdout = NULL
            else:
                print "ONN"
            # This is where processing happens
            hko.good =_process(init, proc, cursor, dest_col)
            if not verbose & 1:
                sys.stdout = oldstdout
            hko.state = 'done'
            hko.time = datetime.datetime.utcnow()
            hko.save()
        else:
            if verbose & 2: print "race lost -- skipping"
#         if verbose & 2: print "sleep..."
        sys.stdout.flush()
        time.sleep(0.001)
#
# balance chunk size vs async efficiency etc
# min 10 obj per chunk
# max 600 obj per chunk
# otherwise try for at least 10 chunks per proc
#
def _calc_chunksize(count, multi):
    cs = count/(multi*10.0)
#     if verbose & 2: print "\ninitial size:", cs
    cs = max(cs, 10)
    cs = min(cs, 600)
    if count / float(cs * multi) < 1.0:
        cs *= count / float(cs * multi)
        cs = max(1, int(cs))
#     if verbose & 2: print "obj count:", count
#     if verbose & 2: print "multi proc:", multi
#     if verbose & 2: print "chunk size:", cs
#     if verbose & 2: print "chunk count:", count / float(cs)
#     if verbose & 2: print "per proc:", count / float(cs * multi)
    return cs

# cs = _calc_chunksize(11, 3)
# cs = _calc_chunksize(20, 1)
# cs = _calc_chunksize(1000, 5)
# cs = _calc_chunksize(1000, 15)
# cs = _calc_chunksize(1000, 150)
# cs = _calc_chunksize(100000, 5)
# cs = _calc_chunksize(100000, 15)
# cs = _calc_chunksize(100000, 150)
# exit()

def mmap(   cb,
            source_col,
            dest_col,
            cb_init=None, 
            source_uri="mongodb://127.0.0.1/test", 
            dest_uri="mongodb://127.0.0.1/test",
            query={},
            key='_id',
            verbose=1,
            multi=None,
            init=True,
            defer=False,
            mongoengine=False):
    dbs = pymongo.MongoClient(source_uri).get_default_database()
    dbd = pymongo.MongoClient(dest_uri).get_default_database()
    dest = dbd[dest_col]
    if (not defer) and multi == None:           #don't use housekeeping, run straight process
        source = dbs[source_col].find(query)
        _process(cb_init, cb, source, dest, verbose)
    else:
        if init:
            chunk_size = _calc_chunksize(dbs[source_col].count(), multi)
            if verbose & 2: print "chunk size:", chunk_size
            _init(dbs[source_col], dest, key, query, chunk_size, verbose)
        if not defer:
            if sys.argv[0] == "" or sys.argv[0][-8:] == "/ipython":
                print >> sys.stderr, ("WARNING -- can't generate module name. Multiprocessing will be emulated...")
                _do_chunks(cb_init, cb, dbs[source_col], dest, query, key, verbose)
            else:
                cb_mod = sys.argv[0][:-3]
                cmd = "python worker.py %s %s %s %s --src_uri='%s' --dest_uri='%s' --query='%s' --key=%s --verbose=%s &" % (cb_mod, cb.__name__, source_col, dest_col,
                                                                source_uri, dest_uri, query, key, verbose)
                if verbose & 2: print "os.system:", cmd
                for j in range(multi):
                    os.system(cmd)
    return dbd[dest_col]

def toMongoEngine(pmobj, metype):
    meobj = metype()
    for key in pmobj:
        if hasattr(meobj, key) and key != '_id' and key in pmobj:
            meobj[key] = pmobj[key]
    meobj[type(meobj).id.name] = pmobj['_id']
    meobj.validate()
    return meobj

def connectMongoEngine(pmcol):
    if pymongo.version_tuple[0] == 2:     #really? REALLY?
        host = pmcol.database.connection.HOST
        port = pmcol.database.connection.PORT
    else:
        host = pmcol.database.client.HOST
        port = pmcol.database.client.PORT
    return meng.connect(pmcol.database.name, host=host, port=port)

def remaining():
    return housekeep.objects(state__ne = "done").count()