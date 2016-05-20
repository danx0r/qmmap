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

if raw_input("drop goosrc?")[:1] == 'y':
    db.goosrc.drop()

for i in range(0, int(config.num), 3):
    db.goosrc.save({'num': i})
print "created %d objects" % db.goosrc.count()

#for debug tests
db.test_mongoo.save({'_id':'dbg1','n':1})