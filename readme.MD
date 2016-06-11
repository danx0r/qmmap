#MongoO

##MongoDB Operations in parallel

MongoO is a lightweight library that enables asynchronous, parallel processing of MongoDB documents using a simple, map-like interface.

Python's `map` function takes a callback function and applies it to a list, returning a list:

```Python
src = [x for x in range(10)]
def func(v):
    return v*10

print map(func, src)
[0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
```

MongoO provides a similar function, ```mmap```, that operates on MongoDB collections:

```Python
# assumes mongodb running locally, database named 'test'
import pymongo
from mongoo import mmap

db = pymongo.MongoClient().test

for i in range(10):
    db.mongoo_src.save({'_id': i})

def func(source):
    return {'_id': source['_id']*10}

ret = mmap(func, "mongoo_src", "mongoo_dest")
print list(ret.find())
[{u'_id': 0}, {u'_id': 10}, {u'_id': 20}, {u'_id': 30}, {u'_id': 40}, {u'_id': 50}, {u'_id': 60}, {u'_id': 70}, {u'_id': 80}, {u'_id': 90}]
```

mongoo has helper functions to support mongoengine classes:

```Python
from mongoengine import Document, IntField, connect
from mongoo import toMongoEngine, connectMongoEngine
connect()

class mongoo_src(Document):
    num = IntField(primary_key = True)

class mongoo_dest(Document):
    val = IntField(primary_key = True)

def init(source, dest):
    connectMongoEngine(dest)

def func(source):
    gs = toMongoEngine(source, mongoo_src)
    gd = mongoo_dest(val = gs.num * 10)
    return gd.to_mongo()

ret = mmap(func, "mongoo_src", "mongoo_dest")

for o in mongoo_dest.objects:
    print o.val,
0 10 20 30 40 50 60 70 80 90
```

We can leverage multiple CPU's by specifying the ```multi``` parameter:

```Python
ret = mmap(func, "mongoo_src", "mongoo_dest", multi=2)
WARNING -- can't generate module name. Multiprocessing will be emulated...

print list(ret.find())
[{u'_id': 10}, {u'_id': 20}, {u'_id': 30}, {u'_id': 40}, {u'_id': 50}, {u'_id': 60}, {u'_id': 70}, {u'_id': 80}, {u'_id': 90}, {u'_id': 100}, {u'_id': 0}]
```

mongoo doesn't (presently) support multiple CPU's from the command line, so we get a warning. Let's run ```test.py``` to see multiple CPU's work for real:

```Python
python test.py
drop mongoo_src, mongoo_dest, housekeeping(mongoo_src_mongoo_dest)?y
Generating test data, this may be slow...
Running mmap...
time processing: 18.1320679188 seconds
representative output:
BQZWVQTIEWZWHNERPLCP
FSLTFLDAYTKHWCHKWTTX
BRYOCRKDJGTBZCKMMSIG
```
On my machine using one process this took about 18 seconds. Let's try to use all 4 cores:
```Python
python test.py 4 --skipdata
drop mongoo_dest, housekeeping(mongoo_src_mongoo_dest)?y
Running mmap...
time processing: 6.23155999184 seconds
representative output:
BQZWVQTIEWZWHNERPLCP
FVPKESQNSFVIHUQQOJCX
UXSIJIMOOHGBFWGGSENP
```
Six seconds - about 3 times faster. Larger data sets should provide even better results; speedup should approach the number of CPU's available.

Multiple separate machines can operate on the same data, as a compute cluster. To accomplish this, we break up the processing into an initialization phase which runs first, then run a process phase on multiple nodes:

```Python
python test.py 4 --skipdata --init_only
drop mongoo_dest, housekeeping(mongoo_src_mongoo_dest)?y
Running mmap...
time processing: 0.20853805542 seconds
representative output:

0 succesful operations out of 10000
```

Now we start processing on two "nodes" (for now, we will test in two shells on the same machine):

Shell 1:
```Python
python test.py 2 --skipdata --process_only --verbose=0
Running mmap...
time processing: 6.0252699852 seconds
representative output:
WXRHAXPHZLYLDQZWPLPS
XVFKIXHPOBTUZFKPMGRD
PVKUCFRLANQXCMCBQKXC

10000 succesful operations out of 10000
```

Shell 2:
```Python
python test.py 2 --skipdata --process_only --verbose=0
Running mmap...
time processing: 4.03092908859 seconds
representative output:
WXRHAXPHZLYLDQZWPLPS
XVFKIXHPOBTUZFKPMGRD
PVKUCFRLANQXCMCBQKXC

10000 succesful operations out of 10000
```

