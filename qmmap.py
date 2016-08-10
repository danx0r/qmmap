#
# mongo Operations
#
import sys, os, importlib, datetime, time, traceback, __main__
import socket

import pymongo
from pymongo.read_preferences import ReadPreference
import threading
import mongoengine as meng
from mongoengine.context_managers import switch_collection

MAX_CHUNK_SIZE = 60  # Overall limit
NULL = open(os.devnull, "w")

def is_shell():
    return sys.argv[0] == "" or sys.argv[0][-8:] == "/ipython"

class housekeep(meng.Document):
    start = meng.DynamicField(primary_key = True)
    end = meng.DynamicField()
    total = meng.IntField()  # total # of entries to do
    good = meng.IntField(default = 0)  # entries successfully processed
#     bad = meng.IntField(default = 0)                    # entries we failed to process to completion
#     log = meng.ListField()                              # log of misery -- each item a failed processing incident
    state = meng.StringField(default = 'open')
    # Globally unique identifier for the process, if any, that is working on
    # this chunk, to know if something else is working on it
    procname = meng.StringField(default = 'none')
#     git = meng.StringField()                                 # git commit of this version of source_destination
    tstart = meng.DateTimeField()  # Time when job started
    time = meng.DateTimeField()  # Time when job finished
    meta = {'indexes': ['state', 'time']}

def _connect(srccol, destcol, dest_uri=None):
    connectMongoEngine(destcol, dest_uri)
    hk_colname = srccol.name + '_' + destcol.name
    switch_collection(housekeep, hk_colname).__enter__()

def _init(srccol, destcol, key, query, chunk_size, verbose):
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
    tot = q.limit(chunk_size).count(with_limit_and_skip=True)
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
        #limit count to chunk for speed
        tot = q.limit(chunk_size).count(with_limit_and_skip=True)


def _is_okay_to_work_on(hkstart):
    """Returns whether a chunk, identified by its housekeeping start value, is okay
to work on, i.e. whether its status is "working" and this process is assigned to it
    """
    if not hkstart:  # Can ignore if specific chunk not specified
        return True
    chunk = housekeep.objects.get(start=hkstart)
    # If it's been reset to open, or being worked on by another node, no good
    state = chunk.state
    if state == "done":
        print "Chunk {0} is already finished".format(hkstart)
        sys.stdout.flush()
        return False
    if state == "open":
        print "Chunk {0} had been reset to open".format(hkstart)
        sys.stdout.flush()
        return False
    if state == "working" and chunk.procname != _procname():
        print "Chunk {0} was taken over by {1}, moving on".format(
            hkstart, chunk.procname)
        sys.stdout.flush()
        return False
    return True


def _process(init, proc, src, dest, verbose, hkstart=None):
    """Run process `proc` on cursor `src`.
    @hkstart: primary key of houskeeping chunk that this is processing, if you are
using one and which to avoid collisions
    """
    if not verbose & 1:
        oldstdout = sys.stdout
        sys.stdout = NULL
    if init:
        try:
            init(src, dest)
        except:
            print >> sys.stderr, "***EXCEPTION (process)***"
            print >> sys.stderr, traceback.format_exc()
            print >> sys.stderr, "***END EXCEPTION***"
            return 0
    good = 0
    inserts = 0
    # Before starting, check if some other process has taken over; in that
    # case, exit early with -1
    if not _is_okay_to_work_on(hkstart):
        return -1
    bulk = dest.initialize_unordered_bulk_op()
    src.batch_size(MAX_CHUNK_SIZE)
    for doc in src:
        try:
            ret = proc(doc)
            if ret != None:
                # If doing housekeeping, save for bulk insert since that will know
                # whether these would be duplicate inserts
                if hkstart:
                    bulk.insert(ret)
                else:
                    # No housekeeping checks, so save immediately with DB check
                    dest.find_and_modify(ret, ret, upsert=True)
                inserts += 1
            good += 1
        except:
            print >> sys.stderr, "***EXCEPTION (process)***"
            print >> sys.stderr, traceback.format_exc()
            print >> sys.stderr, "***END EXCEPTION***"
    # After processing, check again if okay to insert
    sys.stdout.flush()
    if not _is_okay_to_work_on(hkstart):
        return -1
    print >> sys.stderr, "Doing bulk insert of {0} items".format(inserts)
    if hkstart:  # Do bulk insert only if doing housekeeping
        try:
            bulk.execute()
        except:
            print >> sys.stderr, "***EXCEPTION (process)***"
            print >> sys.stderr, traceback.format_exc()
            print >> sys.stderr, "***END EXCEPTION***"
    if not verbose & 1:
        sys.stdout = oldstdout
    return good

def do_chunks(init, proc, src_col, dest_col, query, key, verbose):
    while housekeep.objects(state = 'done').count() < housekeep.objects.count():
        tnow = datetime.datetime.utcnow()
        raw = housekeep._collection.find_and_modify(
            {'state': 'open'},
            {
                '$set': {
                    'state': 'working',
                    'tstart': tnow,
                    'procname': _procname(),
                }
            }
        )
        # if raw==None, someone scooped us
        if raw != None:
            raw_id = raw['_id']
            #reload as mongoengine object -- _id is .start (because set as primary_key)
            hko = housekeep.objects(start = raw_id)[0]
            # Record git commit for sanity
#             hko.git = git.Git('.').rev_parse('HEAD')
#             hko.save()
            # get data pointed to by housekeep
            qq = {'$and': [query, {key: {'$gte': hko.start}}, {key: {'$lte': hko.end}}]}
            # Make cursor not timeout, using version-appropriate paramater
            if pymongo.version_tuple[0] == 2:
                cursor = src_col.find(qq, timeout=False)
            elif pymongo.version_tuple[0] == 3:
                cursor = src_col.find(qq, no_cursor_timeout=True)
            else:
                raise Exception("Unknown pymongo version")
            if verbose & 2: print "mongo_process: %d elements in chunk %s-%s" % (cursor.count(), hko.start, hko.end)
            sys.stdout.flush()
            # This is where processing happens
            hko.good =_process(init, proc, cursor, dest_col, verbose,
                hkstart=raw_id)
            # Check if another job finished it while this one was plugging away
            hko_later = housekeep.objects(start = raw_id).only('state')[0]
            if hko.good == -1:  # Early exit signal
                print "Chunk at %s lost to another process; not updating" % raw_id
                sys.stdout.flush()
            elif hko_later.state == 'done':
                print "Chunk at %s had already finished; not updating" % raw_id
                sys.stdout.flush()
            else:
                hko.state = 'done'
                hko.procname = 'none'
                hko.time = datetime.datetime.utcnow()
                hko.save()
        else:
            # Not all done, but none were open for processing; thus, wait to
            # see if one re-opens
            print 'Standing by for reopening of "working" job...'
            time.sleep(60)


def _num_not_at_state(state):
    """Helper for consisely counting the number of housekeeping objects at a
given state
    """
    return housekeep.objects(state__ne=state).count()


# balance chunk size vs async efficiency etc
# min 10 obj per chunk
# max MAX_CHUNK_SIZE obj per chunk
# otherwise try for at least 10 chunks per proc
#
def _calc_chunksize(count, multi):
    cs = count/(multi*10.0)
#     if verbose & 2: print "\ninitial size:", cs
    cs = max(cs, 10)
    cs = min(cs, MAX_CHUNK_SIZE)
    if count / float(cs * multi) < 1.0:
        cs *= count / float(cs * multi)
        cs = max(1, int(cs))
#     if verbose & 2: print "obj count:", count
#     if verbose & 2: print "multi proc:", multi
#     if verbose & 2: print "chunk size:", cs
#     if verbose & 2: print "chunk count:", count / float(cs)
#     if verbose & 2: print "per proc:", count / float(cs * multi)
    return int(cs)


def _procname():
    """Utility for getting a globally-unique process name, which needs to combine
hostname and process id
@returns: string with format "<fully qualified hostname>:<process id>"."""
    return "{0}:{1}".format(socket.getfqdn(), os.getpid())


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
            init=None, 
            source_uri="mongodb://127.0.0.1/test", 
            dest_uri="mongodb://127.0.0.1/test",
            query={},
            key='_id',
            verbose=1,
            multi=None,
            wait_done=True,
            init_only=False,
            process_only=False,
            manage_only=False,
            timeout=120):

    dbs = pymongo.MongoClient(
        source_uri, read_preference=ReadPreference.SECONDARY_PREFERRED,
    ).get_default_database()
    dbd = pymongo.MongoClient(dest_uri).get_default_database()
    dest = dbd[dest_col]
    if multi == None:  # don't use housekeeping, run straight process

        source = dbs[source_col].find(query)
        _process(init, cb, source, dest, verbose)
    else:
        _connect(dbs[source_col], dest, dest_uri)
        if manage_only:
            manage(timeout)
        elif not process_only:
            chunk_size = _calc_chunksize(dbs[source_col].count(), multi)
            if verbose & 2: print "chunk size:", chunk_size
            _init(dbs[source_col], dest, key, query, chunk_size, verbose)
        # Now process code, if one of the other "only_" options isn't turned on
        if not manage_only and not init_only:
            args = (init, cb, dbs[source_col], dest, query, key, verbose)
            if verbose & 2:
                print "Chunking with arguments %s" % (args,)
            if is_shell():
                print >> sys.stderr, ("WARNING -- can't generate module name. Multiprocessing will be emulated...")
                do_chunks(*args)
            else:
                if multi > 1:
                    for j in xrange(multi):
                        if verbose & 2:
                            print "Launching subprocess %s" % j
                        threading.Thread(target=do_chunks, args=args).start()
                else:
                    do_chunks(*args)
            if wait_done:
                manage(timeout)
                #wait(timeout, verbose & 2)
    return dbd[dest_col]

def toMongoEngine(pmobj, metype):
    meobj = metype._from_son(pmobj)
    meobj.validate()
    return meobj

def connectMongoEngine(pmcol, conn_uri=None):
    if pymongo.version_tuple[0] == 2:     #really? REALLY?
        #host = pmcol.database.connection.HOST
        #port = pmcol.database.connection.PORT
        host = pmcol.database.connection.host
        port = pmcol.database.connection.port
    else:
        host = pmcol.database.client.HOST
        port = pmcol.database.client.PORT
    # Can just use the connection uri, which has credentials
    if conn_uri:
        return meng.connect(pmcol.database.name, host=conn_uri)
    return meng.connect(pmcol.database.name, host=host, port=port)

def remaining():
    return housekeep.objects(state__ne = "done").count()

def wait(timeout=120, verbose=True):
    t = time.time()
    r = remaining()
    rr = r
    while r:
#         print "DEBUG r %f rr %f t %f" % (r, rr, time.time() - t)
        if time.time() - t > timeout:
            if verbose: print >> sys.stderr, "TIMEOUT reached - resetting working chunks to open"
            q = housekeep.objects(state = "working")
            if q:
                q.update(state="open", procname='none')
        if r != rr:
            t = time.time()
        if verbose: print r, "chunks remaning to be processed; %f seconds left until timeout" % (timeout - (time.time() - t)) 
        time.sleep(1)
        rr = r
        r = remaining()


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
            print "%s still waiting: %d out of %d complete (%.3f%%). %.3f seconds complete, %.3f remaining (%.5f hours)" \
            % (datetime.datetime.now().strftime("%H:%M:%S:%f"), done, tot, pdone, tdone, trem, trem / 3600.0)
        else:
            print "No progress data yet"
    else:
        print "%s still waiting; nothing done so far" % (datetime.datetime.now(),)
sys.stdout.flush()


def manage(timeout, sleep=120):
    """Give periodic status, reopen dead jobs, return success when over;
    combination of wait, status, clean, and the reprocessing functions.
    sleep = time (sec) between status updates
    timeout = time (sec) to give a job until it's restarted
    """
    num_not_done = _num_not_at_state('done')
    print "Managing job's execution; currently {0} remaining".format(num_not_done)
    sys.stdout.flush()
    # Keep going until none are state=working or done
    while _num_not_at_state('done') > 0:
        # Sleep before management step
        time.sleep(sleep)
        _print_progress()
        # Now, iterate over state=working jobs, restart ones that have gone
        # on longer than the timeout param
        tnow = datetime.datetime.now()  # get time once instead of repeating
        # Iterate through working objects to see if it's too long
        hkwquery = [h for h in housekeep.objects(state='working').all()]
        for hkw in hkwquery:
            # .tstart must have time value for state to equal 'working' at all
            time_taken = (tnow - hkw.tstart).total_seconds()
            print (u"Chunk on {0} starting at {1} has been working for {2} " +
                u"sec").format(hkw.procname, hkw.start, time_taken)
            sys.stdout.flush()
            if time_taken > timeout:
                print (u"More than {0} sec spent on chunk {1} ;" +
                    u" setting status back to open").format(
                    timeout, hkw.start)
                sys.stdout.flush()
                hkw.state = "open"
                hkw.procname = 'none'
                hkw.save()
    print "----------- PROCESSING COMPLETED ------------"
