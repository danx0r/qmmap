import os, sys, time, random, argparse
import mongoengine as meng
import mongoo

class mongoo_src(meng.Document):
    s1 = meng.StringField()
    s2 = meng.StringField()

class mongoo_dest(meng.Document):
    s = meng.StringField()

def process(source):
    gs = mongoo.toMongoEngine(source, mongoo_src)
    s = ""
    for i in range(len(gs.s1)):
        s += gs.s1[i]
        s += gs.s2[i]
    gd = mongoo_dest(s = s)
    return gd.to_mongo()

def randstring(size):
    s = ""
    for i in range(size):
        s += chr(random.randint(65, 65+25))
    return s
 
if __name__ == "__main__":
    import pymongo, time
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    meng.connect()

    par = argparse.ArgumentParser(description = "Mongoo test large data set")
    par.add_argument("processes", type=int, nargs='?', default=1)
    par.add_argument("size", type=int, nargs='?', default=500)
    par.add_argument("num", type=int, nargs='?', default=10000)
    par.add_argument("--verbose", type=int, default = 1)
    par.add_argument("--skipdata", action='store_true')
    
    config = par.parse_args()

    if not config.skipdata:
        if raw_input("drop mongoo_src, mongoo_dest, housekeeping(mongoo_src_mongoo_dest)?")[:1] == 'y':
            db.mongoo_src.drop()
            db.mongoo_dest.drop()
            db.mongoo_src_mongoo_dest.drop()
    
        print "Generating test data, this may be slow..."
        for i in range(config.num):
            src = mongoo_src()
            src.s1 = randstring(config.size)
            src.s2 = randstring(config.size)
            src.save()
    else:
        if raw_input("drop mongoo_dest, housekeeping(mongoo_src_mongoo_dest)?")[:1] == 'y':
            db.mongoo_dest.drop()
            db.mongoo_src_mongoo_dest.drop()
    
    t = time.time()
    mongoo.mmap(process, "mongoo_src", "mongoo_dest", multi=config.processes, verbose=config.verbose)
    print "time processing:", time.time() - t, "seconds"
    print "representative output:"
#     print list(db.mongoo_dest.find())
    for o in mongoo_dest.objects.limit(2):
        print o.s
    print

    #inspect housekeeping collection for fun & profit
    good = 0
    total = 0
    for hk in db.mongoo_src_mongoo_dest.find():
        good += hk['good']
        total += hk['total']
    print "%d succesful operations out of %d" % (good, total)
