#
# mongo Operations
#
import sys
from datetime import datetime
import mongoengine as meng
from mongoengine.connection import get_db
from mongoengine import register_connection
from mongoengine.context_managers import switch_db

#
# user:passwd@hostipaddr:port/database/collection
#
def parse_concol(s):
    ret = {}
    if '@' in s:
        up, s = s.split('@')
        ret['user'], ret['password']  = up.split(':')
    else:
        ret['user'], ret['password']  = None, None
    if ':' in s:
        ret['host'], s = s.split(':')
    else:
        ret['host'] = "localhost"
    temp = s.split('/')
    if len(temp) == 3:
        ret['port'] = int(temp[0])
        ret['database'] = temp[1]
        ret['collection'] = temp[2] 
    elif len(temp) == 2:
        ret['port'] = 27017
        ret['database'] = temp[0]
        ret['collection'] = temp[1] 
    elif len(temp) == 1:
        ret['port'] = 27017
        ret['database'] = "local_db"
        ret['collection'] = temp[0] 
    else:
        raise Exception("parse_concol format: [user:passwd@][hostipaddr][:port]]/database/collection")
    return ret

def open_concol(cc):
    cc = parse_concol(cc)
#     print "DEBUG cc:", cc
    meng.connect(cc['database'], host=cc['host'], port=cc['port'], username=cc['user'], password=cc['password'])
    return globals()[ cc['collection'] ]
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

    db=meng.connect("local_db", alias="default", host="127.0.0.1", port=27017)
#     print parsed.objects.count(), db
    db2=meng.connect("dev_testdb", alias="dbtoo", host="127.0.0.1", port=8501, username="tester", password=config.password)
#     print parsed.objects.count(), db2
    switch_db(li_profiles, "dbtoo").__enter__()     #this is fine if we're never switching back
    print parsed.objects.count()
    print li_profiles.objects.count()
