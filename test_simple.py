def process(source):
    print "  processed %s" % source['_id']
    return {'_id': source['_id']*10}

if __name__ == "__main__":
    import os, pymongo, mongoo, time

    db = pymongo.MongoClient("mongodb://127.0.0.1/test").get_default_database()

    if raw_input("drop mongoo_src, mongoo_dest, housekeeping(mongoo_src_mongoo_dest)?")[:1] == 'y':
        db.mongoo_src.drop()
        db.mongoo_dest.drop()
        db.mongoo_src_mongoo_dest.drop()

    for i in range(10):
        db.mongoo_src.save({'_id': i})

    ret = mongoo.mmap(process, "mongoo_src", "mongoo_dest", multi=None)
    
    print "output:", list(ret.find())
