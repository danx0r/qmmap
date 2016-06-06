#
# mongoo worker invoked as command-line script
#
import sys
from mongoo import _do_chunks

print sys.argv

_do_chunks()