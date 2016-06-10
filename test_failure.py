import os, sys, time, random
import mongoengine as meng
import mongoo

class mongoo_src(meng.Document):
    num = meng.IntField(primary_key = True)
  
class mongoo_dest(meng.Document):
    val = meng.IntField(primary_key = True)

#this gets called (if defined) before processing each worker
def init(source, dest):         #type(source)=cursor, type(dest)=collection
    print "process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name)
    
def process(source):
    #test random process fail
    if random.random() < .15:
        print >> sys.stderr, "UNFATHOMABLE FAILURE"
        time.sleep(15)
        print >> sys.stderr, "EXEUNT WORKER"
        return

    gs = mongoo.toMongoEngine(source, mongoo_src)
    
    gd = mongoo_dest(val = gs.num * 10)
    print os.getpid(), "  processed %s" % gs.num
    time.sleep(.5) #slow for testing
    return gd.to_mongo()
 
if __name__ == "__main__":
    import pymongo, time
    os.system("python make_source.py mongodb://127.0.0.1/test 33")
    mongoo.mmap(process, "mongoo_src", "mongoo_dest", cb_init=init, multi=10, verbose=3, timeout=10)
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    print "output:"
#     print list(db.mongoo_dest.find())
    for o in mongoo_dest.objects:
        print o.val,
    print
    good = 0
    total = 0

    #inspect housekeeping collection for fun & profit
    for hk in db.mongoo_src_mongoo_dest.find():
        good += hk['good']
        total += hk['total']
    print "%d succesful operations out of %d" % (good, total)
