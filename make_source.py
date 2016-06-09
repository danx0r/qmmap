#
# test mongoo
#
import sys, os, importlib, argparse
import pymongo

par = argparse.ArgumentParser(description = "create test data for mongoo")
par.add_argument("src_db")
par.add_argument("num", type = int)
config = par.parse_args()

db = pymongo.MongoClient(config.src_db).get_default_database()
print "db:", db

if raw_input("drop mongoo_src, mongoo_dest, housekeeping(mongoo_src_mongoo_dest)?")[:1] == 'y':
    db.mongoo_src.drop()
    db.mongoo_dest.drop()
    db.mongoo_src_mongoo_dest.drop()

for i in range(0, int(config.num), 3):
    db.mongoo_src.save({'_id': i})
print "created %d objects" % db.mongoo_src.count()
