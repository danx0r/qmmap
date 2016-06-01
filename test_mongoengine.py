import mongoo

# meng?
import mongoengine as meng
  
class goosrc(meng.Document):
    _id = meng.IntField(primary_key = True)
  
class goodest(meng.Document):
    _id = meng.IntField(primary_key = True)

def init(source, dest):         #type(source)=cursor, type(dest)=collection
    print "process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name)
    mongoo.connectMongoEngine(dest)
    
def process(source, dest):      #type(source)=document, type(dest)=collection
    if source['_id'] == 6:
        0/0
    gs = mongoo.toMongoEngine(source, goosrc)
    gd = goodest(id = gs.id * 10)
    gd.save()
    print "  processed %s" % gs.id

if __name__ == "__main__":
    import os, pymongo
    os.system("python make_goosrc.py mongodb://127.0.0.1/test 32")
    mongoo.process("goosrc", "goodest", multi=3, defer=True)
    mongoo.process("goosrc", "goodest", multi=3, init=False)
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    print "output:"
    print list(db.goodest.find())
    good = 0
    total = 0
    for hk in db.goosrc_goodest.find():
        good += hk['good']
        total += hk['total']
    print "%d succesful operations out of %d" % (good, total)