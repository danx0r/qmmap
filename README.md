mongoo
=======

goo to bind mongodb to CPU power

mongoo(cb, srccol, srcdb, arg1, arg2..., key=val, key2=val2)

iterates over srcdb/srccol using keyword args as query
makes concurrent & sequential calls to cb with cursor set to ranges of srcdb/srccol
calls cb(cursor, arg1, arg2...)	where cursor = src.objects(key=val, key2=val2)

arg1, arg2 may be additional collections (connected to specific host/db combos)
