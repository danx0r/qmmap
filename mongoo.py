#
# mongo Operations
#
import sys, os
from datetime import datetime
from urlparse import urlparse
import mongoengine as meng
from mongoengine.connection import get_db
from mongoengine import register_connection
from mongoengine.context_managers import switch_db
import config

def connect2db(col, uri):
    parts = urlparse(uri)
    print parts
    db = os.path.basename(parts[2])
    alias = col._class_name + "_"+ db
    print alias
    meng.connect(db, alias=alias, host=uri)
    switch_db(col, alias).__enter__()

class parsed(meng.Document):
    pass
class li_profiles(meng.Document):
    pass

meng.connect("__neverMIND__", host="127.0.0.1", port=27017)
connect2db(parsed, "mongodb://tester:%s@127.0.0.1:8501/dev_testdb" % config.password)
connect2db(li_profiles, "mongodb://127.0.0.1/local_db")
print parsed.objects.count()
print li_profiles.objects.count()
exit()

#
# mongoo(concol1, [concol2, ...] cb, [qarg1=val1, ...])
# call with source connection/collection, dest concol, callback, args for mongoengine query
#
def mongoo(src, *args, **kw):
    src = open_concol(src)
#     print "source:", src
    cb = args[-1]
#     print "mongoo args:", args[:-1]
#     print "mongoo kw:", kw
    cb(src, *args[:-1], **kw)

if __name__ == "__main__":
    import config
    if len(sys.argv) > 1:
        exec(sys.argv[1])
        exit()
    class parsed(meng.Document):
        pass
    class li_profiles(meng.Document):
        pass
    def goosrc_cb(src, *args, **kw):
        print "goosrc_cb:", src, args, kw, src.objects.count()

#     print "DEBUG A"
#     mongoo ("tester:%s@127.0.0.1:8501/dev_testdb/parsed" % config.password, goosrc_cb, flaggy=2)
#     print "DEBUG B"
#     mongoo ("parsed", goosrc_cb, flaggy=1)
#     print "DEBUG C"

    meng.connect("__neverMIND__", host="127.0.0.1", port=27017)
    meng.connect("local_db", alias="local_db", host="127.0.0.1", port=27017)
    switch_db(parsed, "local_db").__enter__()     #this is fine if we're never switching back
    meng.connect("dev_testdb", alias="dev_testdb", host="127.0.0.1", port=8501, username="tester", password=config.password)
    switch_db(li_profiles, "dev_testdb").__enter__()     #this is fine if we're never switching back
    print parsed.objects.count()
    print li_profiles.objects.count()
