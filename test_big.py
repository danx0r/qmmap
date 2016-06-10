import os, sys, time, random
import mongoengine as meng
import mongoo

SIZE = 500
NUM = 10000

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

def randstring():
    s = ""
    for i in range(SIZE):
        s += chr(random.randint(65, 65+25))
    return s
 
if __name__ == "__main__":
    import pymongo, time
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    meng.connect()

    if raw_input("drop mongoo_src, mongoo_dest, housekeeping(mongoo_src_mongoo_dest)?")[:1] == 'y':
        db.mongoo_src.drop()
        db.mongoo_dest.drop()
        db.mongoo_src_mongoo_dest.drop()

    for i in range(NUM):
        src = mongoo_src()
        src.s1 = randstring()
        src.s2 = randstring()
        src.save()
    
    t = time.time()
    mongoo.mmap(process, "mongoo_src", "mongoo_dest", multi=5, verbose=3)
    mongoo.wait()
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
