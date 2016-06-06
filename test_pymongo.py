def process(source):
    print "  processed %s" % source['_id']
    return {'_id': source['_id']*10}

if __name__ == "__main__":
    import os, pymongo, mongoo
    os.system("python make_goosrc.py mongodb://127.0.0.1/test 10")
    mongoo.mmap(process, "goosrc", "goodest", multi=1)
    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()
    print "output:"
    print list(db.goodest.find())
