import mongoengine as meng
import mongoo
import os, time

class goosrc(meng.Document):
    _id = meng.IntField(primary_key = True)
  
class goodest(meng.Document):
    _id = meng.IntField(primary_key = True)

def init(source, dest):         #type(source)=cursor, type(dest)=collection
    print "process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name)
    mongoo.connectMongoEngine(dest)
    
def process(source):
    if source['_id'] == 6:
        0/0
    gs = mongoo.toMongoEngine(source, goosrc)
    gd = goodest(id = gs.id * 10)
    print os.getpid(), "  processed %s" % gs.id
    time.sleep(.5) #slow for testing
    return gd.to_mongo()
 
if __name__ == "__main__":
    import pymongo, time
    print "DEBUG test_mongoengine pid:", os.getpid()
    os.system("python make_goosrc.py mongodb://127.0.0.1/test 32")
    mongoo.mmap(None, "goosrc", "goodest", init, multi=3, defer=True)
    mongoo.mmap(process, "goosrc", "goodest", multi=3, init=False)
    r = mongoo.remaining()
    while r:
        print r, "chunks remaning to be processed"
        time.sleep(.25)
        r = mongoo.remaining()
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    print "output:"
    print list(db.goodest.find())
    good = 0
    total = 0
    for hk in db.goosrc_goodest.find():
        good += hk['good']
        total += hk['total']
    print "%d succesful operations out of %d" % (good, total)