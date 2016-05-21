import mongoo

# meng?
import mongoengine as meng
  
class goosrc(meng.Document):
    _id = meng.IntField(primary_key = True)
  
class goodest(meng.Document):
    _id = meng.IntField(primary_key = True)
  
def process(source, dest):
    print "process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name)
    mongoo.connectMongoEngine(dest)
    for doc in source:
        gs = mongoo.toMongoEngine(doc, goosrc)
        gd = goodest(id = gs.id * 10)
        gd.save()
        print "  processed %s" % gs.id

if __name__ == "__main__":
    import os, pymongo
    os.system("python make_goosrc.py mongodb://127.0.0.1/test 10")
    mongoo.process(process, "goosrc", "goodest")
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    print "output:"
    print list(db.goodest.find())
