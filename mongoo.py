#
# mongo Operations
#
import sys, importlib
import pymongo
import mongoengine as meng

# Ooh, it's magic, I know...
caller = importlib.import_module(sys.argv[0][:-3])

def process(source_col, 
            dest_col, 
            source_uri="mongodb://127.0.0.1/test", 
            dest_uri="mongodb://127.0.0.1/test",
            source_args={}):
    dbs = pymongo.MongoClient(source_uri).get_default_database()
    dbd = pymongo.MongoClient(dest_uri).get_default_database()
    source = dbs[source_col].find(source_args)
    dest = dbd[dest_col]
    caller.process(source, dest)
    
def toMongoEngine(pmobj, metype):
    meobj = metype()
    for key in pmobj:
        if hasattr(meobj, key) and key in pmobj:
            meobj[key] = pmobj[key]
    return meobj

def connectMongoEngine(pmcol):
    return meng.connect(pmcol.database.name,
                        host=pmcol.database.client.HOST, 
                        port=pmcol.database.client.PORT)
