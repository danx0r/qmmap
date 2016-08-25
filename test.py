import os, sys, time, random, argparse
import mongoengine as meng
import qmmap

class qmmap_src(meng.Document):
    s1 = meng.StringField()
    s2 = meng.StringField()

class qmmap_dest(meng.Document):
    s = meng.StringField()

# if defined, this will run at start of each chunk
def init(source_cursor, destination_collection):
#     print "processing %d documents from %s to %s" % (source_cursor.count(), source_cursor.collection.name, destination_collection.name)
    pass

def process(source):
    gs = qmmap.toMongoEngine(source, qmmap_src)
    s = ""
    for i in range(len(gs.s1)):
        s += gs.s1[i]
        s += gs.s2[i]
    gd = qmmap_dest(s = s)
    return gd.to_mongo()

def randstring(size):
    s = ""
    for i in range(size):
        s += chr(random.randint(65, 65+25))
    return s
 
if __name__ == "__main__":
    import pymongo, time
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    meng.connect("test")

    par = argparse.ArgumentParser(description = "qmmap test large data set")
    par.add_argument("processes", type=int, nargs='?', default=1)
    par.add_argument("size", type=int, nargs='?', default=500)
    par.add_argument("num", type=int, nargs='?', default=10000)
    par.add_argument("--verbose", type=int, default=1)
    par.add_argument("--skipdata", action='store_true')
    par.add_argument("--init_only", action='store_true')
    par.add_argument("--process_only", action='store_true')
    par.add_argument("--timeout", type=int, default=120)
    par.add_argument("--sleep", type=int, default=60)
    
    config = par.parse_args()

    if not config.process_only:
        if not config.skipdata:
            if raw_input("drop qmmap_src, qmmap_dest, housekeeping(qmmap_src_qmmap_dest)?")[:1] == 'y':
                db.qmmap_src.drop()
                db.qmmap_dest.drop()
                db.qmmap_src_qmmap_dest.drop()
        
            print "Generating test data, this may be slow..."
            for i in range(config.num):
                src = qmmap_src()
                src.s1 = randstring(config.size)
                src.s2 = randstring(config.size)
                src.save()
        else:
            if raw_input("drop qmmap_dest, housekeeping(qmmap_src_qmmap_dest)?")[:1] == 'y':
                db.qmmap_dest.drop()
                db.qmmap_src_qmmap_dest.drop()
    
    print "Running mmap..."
    t = time.time()
    qmmap.mmap(burro.halb.process, "qmmap_src", "qmmap_dest", init=init,
        multi=config.processes, verbose=config.verbose, init_only=config.init_only,
        process_only=config.process_only, timeout=config.timeout,
        sleep=config.sleep)
    print "time processing:", time.time() - t, "seconds"
    print "representative output:"
#     print list(db.qmmap_dest.find())
    for o in qmmap_dest.objects.limit(3):
        print o.s[:20]
    print

    #inspect housekeeping collection for fun & profit
    good = 0
    total = 0
    for hk in db.qmmap_src_qmmap_dest.find():
        good += hk['good']
        total += hk['total']
    print "%d succesful operations out of %d" % (good, total)
