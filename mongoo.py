#!/usr/bin/env python
# mongo Operations
#
# mongoo.py src_db source dest_db dest cmd
#
import sys, os, time, datetime, importlib, argparse
from enum import Enum
# from datetime import datetime
from urlparse import urlparse
import pymongo
import mongoengine as meng
# from mongoengine.connection import get_db
# from mongoengine import register_connection
from mongoengine.context_managers import switch_db
from mongoengine.context_managers import switch_collection
from extras_mongoengine.fields import StringEnumField
import git

# #we need class defs from science (at least for pp)
# # PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../science") )     #science is parallel
# PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../") )        #we are located in science/venv/src
# sys.path.append(PYBASE)
# sys.path.append(os.getcwdu())
# from utils.pp import pp
# from sciClasses.index_fix import index_fix

MIN_CHUNK_SIZE = 3
MAX_CHUNK_SIZE = 600

MYID = "%05d-%s" % (os.getpid(), repr(time.time()*1000000)[-8:-2])

NULL = open(os.devnull, "w")

def t0():
    global T
    print MYID, "---PROFILING---"
    sys.stdout.flush()
    T = time.time()

def t1(s=''):
    print MYID, "---%s -- seconds %s---" % (s, time.time() - T)
    sys.stdout.flush()

class hkstate(Enum):
    open = 'open'
    working = 'working'
    fail = 'fail'
    done = 'done'

class housekeep(meng.Document):
    start = meng.DynamicField(primary_key = True)
    end = meng.DynamicField()
    total = meng.IntField()                             # total # of entries to do
    good = meng.IntField(default = 0)                   # entries successfully processed
    bad = meng.IntField(default = 0)                    # entries we failed to process to completion
    log = meng.ListField()                              # log of misery -- each item a failed processing incident
    state = StringEnumField(hkstate, default = 'open')
    git = meng.StringField()                                 # git commit of this version of source_destination
    tstart = meng.DateTimeField()  # Time when job started
    time = meng.DateTimeField()  # Time when job finished
    meta = {'indexes': ['state', 'time']}

connect2db_cnt = 0

def connect2db(col, uri):
    global connect2db_cnt, con
    parts = urlparse(uri)
    db = os.path.basename(parts[2])
    if connect2db_cnt:
        alias = col._class_name + "_"+ db
#         print "DBG alias:", alias
        con = meng.connect(db, alias=alias, host=uri, read_preference=pymongo.ReadPreference.NEAREST)
        switch_db(col, alias).__enter__()
    else:
        con = meng.connect(db, host=uri, read_preference=pymongo.ReadPreference.NEAREST)
    connect2db_cnt += 1
    return con

#
# drop collections and reset
#
def mongoo_reset(srccol, destcol=None):
    print MYID, "dropping housekeeping collection:", housekeep._get_collection_name()
    housekeep.drop_collection()
#     index_fix(housekeep)
    if destcol:
        print MYID, "dropping destination collection:", destcol
        destcol.drop_collection()
#         index_fix(destcol)
    time.sleep(1)
#
# Clean housekeeping -- reset unfinished chunks
#
def mongoo_clean():
    print MYID, "cleaning housekeeping collection:", housekeep._get_collection_name()
    q = housekeep.objects(state__ne = "done")
    n = q.update(set__state = "open", multi = True)
    print MYID, "updated %d objects to state=open" % n
    time.sleep(1)
#
# set up housekeeping
#
#FIXME: destcol not used
def mongoo_init(srccol, destcol, key, query, chunks):
    query = dict(query)                                 #this gets abused, avoid side fx
    if housekeep.objects.count() == 0:
        q = srccol.objects(**query).only(key).order_by(key)
        print MYID, "initializing %d entries, housekeeping for %s" % (q.count(), housekeep._get_collection_name())
    else:
        last = housekeep.objects().order_by('-start')[0].end
        print MYID, "last partition field in housekeep:", last
        query[key + "__gt"] = last
        q = srccol.objects(**query).only(key).order_by(key)
        print MYID, "added %d entries to %s" % (q.count(), housekeep._get_collection_name())
        sys.stdout.flush()
    
    chunk = q.count() / chunks
    if chunk < MIN_CHUNK_SIZE:
        chunk = MIN_CHUNK_SIZE
    if chunk > MAX_CHUNK_SIZE:
        chunk = MAX_CHUNK_SIZE
    print MYID, "chunk size:", chunk
    sys.stdout.flush()

    i = 0
    tot = q.limit(chunk).count()
    while tot > 0:
        print MYID, "housekeeping: %d" % i
        i +=1
        sys.stdout.flush()
        hk = housekeep()
        hk.start = getattr(q[0], key)
        hk.end =  getattr(q[min(chunk-1, tot-1)], key)
        if (hk.start == None or hk.end == None):
            print >> sys.stderr, "ERROR: key field has None. start: %s end: %s" % (hk.start, hk.end)
            raise Exception("key error")
        #calc total for this segment
        query[key + "__gte"] = hk.start
        query[key + "__lte"] = hk.end
        hk.total = srccol.objects(**query).count()
        del query[key + "__gte"]
        del query[key + "__lte"]
        hk.save()

        #get start of next segment
        query[key + "__gt"] = hk.end
        q = srccol.objects(**query).only(key).order_by(key)
        del query[key + "__gt"]
        tot = q.limit(chunk).count()                    #limit count to chunk for speed
    init = True

#
# Process what we can
#
def mongoo_process(srccol, destcol, key, query, cb, verbose):
    alldone = housekeep.objects.count()
    while housekeep.objects(state = 'done').count() != alldone:
        #
        # tricky pymongo stuff mongoengine doesn't support.
        # find an open chunk, update it to state=working
        # must be done atomically to avoid contention with other processes
        #
        # update housekeep.state and .tstart with find and modify
        tnow = datetime.datetime.now()
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
            hko.git = git.Git('.').rev_parse('HEAD')
            hko.save()
            # get data pointed to by housekeep
            query[key + "__gte"] = hko.start
            query[key + "__lte"] = hko.end
            cursor = srccol.objects(**query)
            print MYID, "%s mongo_process: %d elements in chunk %s-%s" % (datetime.datetime.now().strftime("%H:%M:%S:%f"), cursor.count(), hko.start, hko.end)
            sys.stdout.flush()
            if not verbose:
                oldstdout = sys.stdout
                sys.stdout = NULL
            # This is where processing happens
            hko.good, hko.bad, hko.log = cb(cursor, destcol, MYID)
            if not verbose:
                sys.stdout = oldstdout
            hko.state = 'done'
            hko.time = datetime.datetime.now()
            hko.save()
        else:
            print MYID, "race lost -- skipping"
#         print MYID, "sleep..."
        sys.stdout.flush()
        time.sleep(WAITSLEEP)
    print MYID, "mongo_process over"

def mongoo_status():
    print MYID, "----------- TRACKING STATUS ------------"
    print MYID, "%s done, %s not" % (housekeep.objects(state = 'done').count(), housekeep.objects(state__ne = 'done').count())
    bad = 0
    good = 0
    tot = 0
    for h in housekeep.objects:
        tot += h.total
    for h in housekeep.objects(state = 'done'):
        bad += h.bad
        good += h.good
        if h.total != h.good:
            print MYID, "Some badness found for %s-%s:" % (h.start, h.end)
            print MYID, "%d are good, %d are bad." % (h.good, h.bad)
            for badd in h.log:
                print MYID, "----------------------------------------"
                print MYID, badd
                print MYID, "----------------------------------------"
    print MYID, "total good: %d bad: %d sum: %d expected total: %d" % (good, bad, good+bad, tot)         


def _print_progress():
    q = housekeep.objects(state = 'done').only('time')
    tot = housekeep.objects.count()
    done = q.count()
    if done > 0:
        pdone = 100. * done / tot
        q = q.order_by('time')
        first = q[0].time
        q = q.order_by('-time')
        last = q[0].time
        if first and last:  # guard against lacking values
            tdone = float((last-first).seconds)
            ttot = tdone*tot / done
            trem = ttot - tdone
            print "%s %s still waiting: %d out of %d complete (%.3f%%). %.3f seconds complete, %.3f remaining (%.5f hours)" \
            % (MYID, datetime.datetime.now().strftime("%H:%M:%S:%f"), done, tot, pdone, tdone, trem, trem / 3600.0)
        else:
            print MYID, "No progress data yet"
    else:
        print "%s %s still waiting; nothing done so far" % (MYID, datetime.datetime.now())
    sys.stdout.flush()

def _num_at_state(state):
    """Helper for consisely counting the number of housekeeping objects at a
given state
    """
    return housekeep.objects(state=state).count()


def mongoo_manage(sleep, timeout):
    """Give periodic status, reopen dead jobs, return success when over;
    combination of wait, status, clean, and the reprocessing functions.
    sleep = time (sec) between status updates
    timeout = time (sec) to give a job until it's restarted
    """
    num_done = _num_at_state('done')
    print MYID, "Managing job's execution; currently {0} done".format(num_done)
    sys.stdout.flush()
    # Keep going until none are state=working or done
    while _num_at_state('open') > 0 or _num_at_state('working') > 0:
        # Sleep before management step
        time.sleep(sleep)
        _print_progress()
        # Now, iterate over state=working jobs, restart ones that have gone
        # on longer than the timeout param
        tnow = datetime.datetime.now()  # get time once instead of repeating
        # Iterate through working objects to see if it's too long
        hkwquery = housekeep.objects(state='working')  #.only('tstart', 'start') see OPS-174
        for hkw in hkwquery:
            # .tstart must have time value for state to equal 'working' at all
            # See mongoo_process
            time_taken = (tnow-hkw.tstart).total_seconds()
            print MYID, u"Chunk starting at {0} has been working for {1} sec".format(
                hkw.start, time_taken)
            sys.stdout.flush()
            if time_taken > timeout:
                print MYID, (u"More than {0} sec spent on chunk {1} ;" +
                    u" setting status back to open").format(
                    timeout, hkw.start)
                sys.stdout.flush()
                hkw.state = "open"
                hkw.save()
    time.sleep(5)
    print MYID, "----------- PROCESSING COMPLETED ------------"
    mongoo_status()


if __name__ == "__main__":
    par = argparse.ArgumentParser(description = "Mongo Operations")
    par.add_argument("src_db")
    par.add_argument("source")
    par.add_argument("dest_db")
    par.add_argument("dest")
    par.add_argument("cmd")
    par.add_argument("--chunk", type=int, default = 3)
    par.add_argument("--multi", type=int, default = 1)
    par.add_argument("--sleep", type=float, default = 1)
    par.add_argument("--timeout", type=int, default = 10)
    par.add_argument("--verbose", type=int, default = 1)
    par.add_argument("--wait_and_reprocess", action = "store_true")
    config = par.parse_args()
#     if len(sys.argv) > 1 and 'config=' in sys.argv[1]:
#         config = importlib.import_module(sys.argv[1][7:])
#     else:
#         import config

    if config.dest_db == '.':
        config.dest_db = config.src_db    
    if config.source == config.dest:
        raise Exception("Source and destination must be different collections")
    print "MYID:", MYID
    print MYID, "CMD:", sys.argv
    src_dest = config.source + "_" + config.dest
    goo = importlib.import_module(src_dest)
    source = getattr(goo, config.source)
    dest = getattr(goo, config.dest)
    WAITSLEEP = config.sleep

    print MYID, "source database, collection:", config.src_db, source
    print MYID, "destination database, collection:", config.dest_db, dest
    
    connect2db(source, config.src_db)
    connect2db(dest, config.dest_db)
    connect2db(housekeep, config.dest_db)
    hk_colname = source._class_name + '_' + dest._class_name
    switch_collection(housekeep, hk_colname).__enter__()

    if hasattr(goo, 'QUERY'):
        query = goo.QUERY
    else:
        query = {}

    t0()

    if 'reset' == config.cmd:
        print MYID, "drop housekeep(%s) and %s at %s, sure?" % (hk_colname, config.dest, config.dest_db)
        if raw_input()[:1] == 'y':
            mongoo_reset(source, dest)
            if hasattr(goo, 'reset'):
                goo.reset(source, dest, MYID)

    elif 'reset_keepdest' == config.cmd:
        print MYID, "drop housekeep(%s) at %s, sure?" % (hk_colname, config.dest_db)
        if raw_input()[:1] == 'y':
            mongoo_reset(source, None)
            if hasattr(goo, 'reset'):
                goo.reset(source, dest, MYID)

    elif 'init' == config.cmd:
        mongoo_init(source, dest, goo.KEY, query, config.chunk)
        if hasattr(goo, 'init'):
            goo.init(source, dest, MYID)
        
    elif 'process' == config.cmd:
        #either simple process or wait, check, reproc & wait again if necessary
        do = False
        if not config.wait_and_reprocess:
            do = True
        else:
            done = mongoo_wait(config.timeout)
            if not done:
                mongoo_clean()
                do = True
        if do:
            if config.multi > 1:
                for i in range(config.multi):
                    #FIXME -- sleep takes %d but is a float. Might slow things down to fix
                    do = "python %s %s %s %s %s --sleep %d --verbose %d process &" % (sys.argv[0], 
                        #why why why why why                                                 
                        config.src_db.replace('$', "\\$"), config.source, config.dest_db.replace('$', "\\$"), config.dest, config.sleep, config.verbose)
                    print MYID, "doing:", do
                    sys.stdout.flush()
                    os.system(do)
            else:
                mongoo_process(source, dest, goo.KEY, query, goo.process, config.verbose)
        if config.wait_and_reprocess:
            if not mongoo_wait(config.timeout):
                t1()
                exit(99)

    elif 'manage' == config.cmd:
        mongoo_manage(config.sleep, config.timeout)

    elif 'dev' == config.cmd:
        WAITSLEEP = 0
        print MYID, "drop housekeep(%s) and %s at %s, sure?" % (hk_colname, config.dest, config.dest_db)
        if raw_input()[:1] == 'y':
            mongoo_reset(source, dest)
            if hasattr(goo, 'reset'):
                goo.reset(source, dest)
        mongoo_init(source, dest, goo.KEY, query, config.chunk)
        if hasattr(goo, 'init'):
            goo.init(source, dest, MYID)
        mongoo_process(source, dest, goo.KEY, query, goo.process)
        mongoo_wait()
        mongoo_status()

    else:
        print "usage:"
        print "mongoo.py reset   #erase all destination data for complete reprocessing"
        print "mongoo.py init    #initialize for processing"
        print "mongoo.py process #process data"
        print "mongoo.py manage  #monitor progress, retry dead jobs"
        print "mongoo.py dev     #reset, init, process in single thread for development tests"
        exit()
        
    t1()
