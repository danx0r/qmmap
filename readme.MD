#QMmap

##Distributed MongoDB Map

###from hiQ Labs (www.hiqlabs.com)

QMmap is a lightweight library that enables asynchronous, parallel processing of MongoDB documents using a simple, map-like interface.

*The following examples were created in IPython, an excellent tool. Cut and paste these examples into your IPython notebook:*

Python's `map` function takes a callback function and applies it to a list, returning a list:

```Python
src = [x for x in range(10)]
def func(v):
    return v*10

print map(func, src)

```
*IPython output:*

    [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]

QMmap provides a similar function, ```mmap```, that operates on MongoDB collections:

```Python
# assumes mongodb running locally, database named 'test', function input is a
# pymongo object
import pymongo
from qmmap import mmap

db = pymongo.MongoClient().test

for i in range(10):
    db.qmmap_in.save({'_id': i})

def func(source):
    return {'_id': source['_id']*10}

ret = mmap(func, "qmmap_in", "qmmap_out")
print list(ret.find())
```
    [{u'_id': 0}, {u'_id': 10}, {u'_id': 20}, {u'_id': 30}, {u'_id': 40}, {u'_id': 50}, {u'_id': 60}, {u'_id': 70}, {u'_id': 80}, {u'_id': 90}]

QMmap has helper functions to support mongoengine classes:

```Python
from mongoengine import Document, IntField, connect
from qmmap import toMongoEngine, connectMongoEngine
connect("test")

class qmmap_in(Document):
    num = IntField(primary_key = True)

class qmmap_out(Document):
    val = IntField(primary_key = True)

def init(source, dest):
    connectMongoEngine(dest)

def func(source):
    gs = toMongoEngine(source, qmmap_in)
    gd = qmmap_out(val = gs.num * 10)
    return gd.to_mongo()

ret = mmap(func, "qmmap_in", "qmmap_out")

for o in qmmap_out.objects:
    print o.val,
```

    0 10 20 30 40 50 60 70 80 90

Alternatively, if your processing function is written in a way that converts a mongoengine object to another mongoengine object, simply add the `qmmapify` decorator, passing in the input class:

```Python
import qmmap

class qmmap_in(Document):
    num1 = IntField()
    num2 = IntField()

class qmmap_out(Document):
    val = IntField()

def func(qmmap_in_obj):
    output = qmmap_out()
    output.val = qmmap_in_obj.num1 + qmmap_in_obj.num2
    return output

@qmmap.qmmapify(qmmap_in)
def qmmap_func(qmmap_in_obj):
    return func(qmmap_in_obj)

ret = qmmap.mmap(qmmap_func, "qmmap_in", "qmmap_out")
```

You can then continue to have `func` operate in the "mongoengine" world while passing `qmmap_func` to qmmap.

We can leverage multiple CPU's by specifying the ```multi``` parameter:

```Python
ret = mmap(func, "qmmap_in", "qmmap_out", multi=2)
```

To invoke from the command line, you must pass four required arguments in this order:

```
  module                Python module containing the function you wish to
                        apply to the input collection, e.g. foo.bar[...]
  function              Function within `module` that you wish to apply to
                        each member of theinput collection
  source_col            input/source collection, sent to `module`.`function`
                        for processing
  dest_col              output/destination collection to write output
                        documents of `module`.`function` to
```

Other deviations from the default arguments can be specified as indicated in `qmcli.py --help`.  Importantly, you may draw the settings from a JSON file using `--jsonconfig` or a python module with `pyconfig`. Assuming the file above is in `processors/convert.py`, you could invoke it from the command line as:

```Bash
qmcli.py --multi=2 processors.convert qmmap_func qmmap_in qmmap_out
```

Run ``test.py`` to see multiple CPU's work for real.

```Python
run test.py
```

```
drop qmmap_in, qmmap_out, housekeeping(qmmap_in_qmmap_out)?y
Generating test data, this may be slow...
Running mmap...
time processing: 18.1320679188 seconds
representative output:
BQZWVQTIEWZWHNERPLCP
FSLTFLDAYTKHWCHKWTTX
BRYOCRKDJGTBZCKMMSIG
```

On my machine using one process, this took about 18 seconds. Let's try to use all 4 cores:

```Python
run test.py 4 --skipdata
```

```
drop qmmap_out, housekeeping(qmmap_in_qmmap_out)?y
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
run test.py 4 --skipdata --init_only
```

```
drop qmmap_out, housekeeping(qmmap_in_qmmap_out)?y
Running mmap...
time processing: 0.20853805542 seconds
representative output:

0 succesful operations out of 10000
```

Now we start processing on two "nodes" (for now, we will test in two IPython shells on the same machine):

Shell 1:
```Python
run test.py 2 --skipdata --process_only --verbose=0
```

```
Running mmap...
time processing: 6.0252699852 seconds
representative output:
WXRHAXPHZLYLDQZWPLPS
XVFKIXHPOBTUZFKPMGRD
PVKUCFRLANQXCMCBQKXC

10000 succesful operations out of 10000
```

Shell 2 (start this right away):
```Python
run test.py 2 --skipdata --process_only --verbose=0
```

```
Running mmap...
time processing: 4.03092908859 seconds
representative output:
WXRHAXPHZLYLDQZWPLPS
XVFKIXHPOBTUZFKPMGRD
PVKUCFRLANQXCMCBQKXC

10000 succesful operations out of 10000
```

