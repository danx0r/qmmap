import mongoo

def process(source, dest):  #type(source)=cursor, type(dest)=collection
    print "process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name)
    for doc in source:
        dest.save({'_id': doc['_id']*10})
        print "  processed %s" % doc['_id']

if __name__ == "__main__":
    import os, pymongo
    os.system("python make_goosrc.py mongodb://127.0.0.1/test 10")
    mongoo.process("goosrc", "goodest")
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    print "output:"
    print list(db.goodest.find())
