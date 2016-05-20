#
# Template mongoo processing script
#
import sys, os, time, random, traceback
import pymongo
#
# source field to use for partitioning (must be unique and orderable)
#
KEY = '_id'
#
# optional query:
#
# QUERY = {'num__ne': 9}
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
    print myid, "process %d documents from %s to %s" % (len(keylist), source_col, dest_col)
    source = pymongo.MongoClient(source_uri).get_default_database()[source_col]
    dest = pymongo.MongoClient(dest_uri).get_default_database()[dest_col]
    for key in keylist:
        src = source.find_one({KEY: key})
        dest.save({'_id': src[KEY]*10})

if __name__ == "__main__":
    process(1, [0, 3, 6], "mongodb://127.0.0.1/local_db", "goosrc", "mongodb://127.0.0.1/local_db", "goodest")