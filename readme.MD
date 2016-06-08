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

We can leverage multiple CPU's by specifying the ```multi``` parameter:

```Python
ret = mmap(func, "mongoo_src", "mongoo_dest", multi=2)
print list(ret.find())
```

