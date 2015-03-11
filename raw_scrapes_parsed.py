#
# Template mongoo processing script
#
import sys, os, time, traceback, zlib
import mongoengine as meng
PYBASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../science") ) 
sys.path.append(PYBASE)
from process_data.parse_li import parse_li

#
# in real-world app, we would import our source and destination collections
#
from sciClasses.parsed import parsed
from sciClasses.scrapus2 import raw_scrapes

#
# source field to use for partitioning (must be unique and orderable)
#
KEY = 'timestamp'

#
# optional query:
#
# QUERY = {'num__ne': 9}

#
# called on initialization of processing (incremental)
#
# def init():
#     pass

#
# called on full reset
#
# def reset():
#     pass

#
# called for each chunk (concurrent)
# source is mongoengine cursor; dest is mongoengine object class
# both are associated with db connections
#
def process(source, dest):
    log = []
    good = bad = 0
    print "  process %d from" % source.count(), source._collection, "to", dest.objects._collection
    for x in source:
        try:
            print "    raw_scrapes_parsed.process:", x.timestamp, x.id
            sys.stdout.flush()
            d = dest()
            html = zlib.decompress(x.html_zlib)
            print ("  decompressed %d bytes; first: %d last: %d" % 
                   (len(html), ord(html[0]), ord(html[-1])))
            prsd, version = parse_li(html)
            d.version = version
            d.li = prsd
            d.li_pub_url = x.scrapus2_url
            d.timestamp = x.timestamp
            d.save()
            good += 1
        except:
            print "------------ERROR--------------"
            bad += 1
            err = "exception processing %s\n" % x.id
            err += traceback.format_exc()
            log.append(err)
            print err
            print "-------------------------------"
            sys.stdout.flush()
    return good, bad, log
