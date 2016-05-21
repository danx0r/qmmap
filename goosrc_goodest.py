import pymongo, os, urlparse

def mongoo_process(source_col, 
                   dest_col, 
                   source_uri="mongodb://127.0.0.1/test", 
                   dest_uri="mongodb://127.0.0.1/test",
                   source_args={}):
    dbs = pymongo.MongoClient(source_uri)[os.path.basename(urlparse.urlparse(source_uri)[2])]
    dbd = pymongo.MongoClient(dest_uri)[os.path.basename(urlparse.urlparse(dest_uri)[2])]
    source = dbs[source_col].find()
    dest = dbd[dest_col]
    process(source, dest)    

def process(source, dest):  #type(source)=cursor, type(dest)=collection
    print "process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name)
    for doc in source:
        dest.save({'_id': doc['_id']*10})
        print "  processed %s" % doc['_id']

# meng?
# import mongoengine as meng
# 
# class goosrc(meng.Document):
#     _id = meng.IntField(primary_key = True)
# 
# class goodest(meng.Document):
#     _id = meng.IntField(primary_key = True)
# 
# def process(source, dest):
#     print "process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name)
#     meng.connect("test")
#     for doc in source:
# #         gs = goosrc.objects(_id = doc['_id'])[0]
#         gs = goosrc()
#         for key in doc:
#             if hasattr(gs, key) and key in doc:
#                 gs[key] = doc[key]
#         gd = goodest(id = gs.id * 10)
#         gd.save()

if __name__ == "__main__":
    mongoo_process("goosrc", "goodest")