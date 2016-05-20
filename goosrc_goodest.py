#
# Template mongoo processing script
#
import sys, os, time, random, traceback
import pymongo
#
# source field to use for partitioning (must be unique and orderable)
#
KEY = 'num'

#
# optional query:
#
QUERY = {'num__ne': 9}

#
# called on initialization of processing (incremental)
#
# def init():
#     pass

#
# called on full reset
#
# def reset():
#     pass

#
# called for each chunk (concurrent)
# source is mongoengine cursor; dest is mongoengine object class
# both are associated with db connections
#
def process(myid, keylist, source_uri, source_col, dest_uri, dest_col, *args, **kw):
    log = []
    good = bad = 0
    source = pymongo.MongoClient(source_uri).get_default_database()
    dest = pymongo.MongoClient(dest_uri).get_default_database()
    print myid, "  process %d documents from %s,%s to %s,%s" % (len(keylist), source, source_col, dest, dest_col)
    for key in keylist:
        dbg1 = source['test_mongoo'].find_and_modify({'_id':'dbg1'}, {'$inc':{'n':1}})['n']
        if dbg1 == 11:               #emulate untrapped segfault/lost or dead process. It happens
            print myid, "INEXPLICABLE FAIL"
            exit()
        src = source[source_col].find_one({KEY: key})
        try:
            num = src['num']
            if num != 24:
                print myid, "    goosrc_goodest.process:", num
                sys.stdout.flush()
#                 d = dest()
#                 d.numnum = str(x.num*10)
                dest[dest_col].save({'numnum': num})
            else:
                print myid, "    goosrc_goodest: skipping", num
                sys.stdout.flush()
                raise Exception("spurious exception")
            good += 1
        except:
            print myid, "------------ERROR--------------"
            bad += 1
            err = traceback.format_exc()
            log.append(err)
            print myid, err
            print myid, "-------------------------------"
            sys.stdout.flush()
    return good, bad, log

if __name__ == "__main__":
    process(1, [0, 3], "mongodb://127.0.0.1/local_db", "goosrc", "mongodb://127.0.0.1/local_db", "goodest")