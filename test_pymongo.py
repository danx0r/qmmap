import mongoo

def process(source, dest):  #type(source)=document, type(dest)=collection
    dest.save({'_id': source['_id']*10})
    print "  processed %s" % source['_id']

if __name__ == "__main__":
    import os, pymongo
    os.system("python make_goosrc.py mongodb://127.0.0.1/test 10")
    mongoo.process("goosrc", "goodest", multi=1)
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    print "output:"
    print list(db.goodest.find())
