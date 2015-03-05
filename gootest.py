#
# test mongoo
#
import sys, os
import mongoengine as meng
from mongoo import mongoo, mongoo_reset, connect2db

PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../science") ) 
sys.path.append(PYBASE)
from utils.pp import pp
import config

class goosrc(meng.Document):
    num = meng.IntField(primary_key = True)

class goodest(meng.Document):
    numnum = meng.StringField()

connect2db(goosrc, "mongodb://127.0.0.1/local_db")
goosrc.drop_collection()
connect2db(goodest, "mongodb://127.0.0.1/local_db")
goodest.drop_collection()
print "delete goosrc_goodest?"
if raw_input()[:1] == 'y':
    mongoo_reset(goosrc, goodest, "mongodb://127.0.0.1/local_db")

for i in range(0, 50, 3):
    g = goosrc(num = i)
    g.save()

def goosrc_cb(src, dest, init = False, reset = False):
    print "goosrc_cb: source count:", src.count(), "init:", init, "reset:", reset
    if init and reset:
        dest.drop_collection()
        print "init: dropped", dest
    for x in src:
        if x.num != 15:
            print "goosrc_cb process:", x.num
            d = dest()
            d.numnum = str(x.num*2)
            d.save()
        else:
            print "goosrc_cb: skipping", x.num

mongoo( goosrc_cb,
        'num',                                                              #key field for partitioning 
        goosrc,                                                             #source collection
        "mongodb://127.0.0.1/local_db",                                     #source host/database
        goodest,                                                            #destination collection
#             "mongodb://tester:%s@127.0.0.1:8501/dev_testdb" % config.password,  #destination host/database
        "mongodb://127.0.0.1/local_db",
        {"num__ne": 3},                                                    #query params for source (mongoengine)
        reset = True                                                        #extra parameters for callback
    )

print "goodest:"
pp(goodest.objects)
