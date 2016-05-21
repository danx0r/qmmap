import mongoo

def process(source, dest):  #type(source)=cursor, type(dest)=collection
    print "process %d documents from %s to %s" % (source.count(), source.collection.name, dest.name)
    for doc in source:
        dest.save({'_id': doc['_id']*10})
        print "  processed %s" % doc['_id']

# # meng?
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
#         gs = goosrc()
#         mongoo.toMongoEngine(doc, gs)
#         gd = goodest(id = gs.id * 10)
#         gd.save()

if __name__ == "__main__":
    mongoo.process(process, "goosrc", "goodest")
