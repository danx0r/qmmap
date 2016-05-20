import pymongo

def process(source, dest):  #type(source)=cursor, type(dest)=collection
    print "process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name)
    for doc in source:
        dest.save({'_id': doc['_id']*10})
        print "  processed %s" % doc['_id']

if __name__ == "__main__":
    db = pymongo.MongoClient().test
    source = db.goosrc.find()
    dest = db.goodest
    process(source, dest)