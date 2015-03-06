mongoo
=======

goo to bind mongodb to CPU power

housekeeping:

dest db used
collection = sourceclassname_destclassname


config.py:
config.src_db = mongodb://usr:pw@host:port/srcdb 
config.source = goosrc
config.dest_db = mongodb://usr:pw@host:port/destdb
config.dest = goodest 
config.test = False									#if true, small chunks, delays to trigger race conditions...

goosrc_goodest.py:
init()
reset()
process()
KEY 												#partition field
QUERY												#query filter to apply

command line:
mongoo.py reset 									#reset everything -- delete dest collection & housekeeping
mongoo.py init 										#initialize housekeeping for processing (check for new items in source etc)
mongoo.py process 									#process source chunks -- concurrent, any # of instances, machines
mongoo.py track 									#report progress, problems
