#!/usr/bin/python
import os
os.system("rm log1 log2 log3 log4")
os.system("echo y | python make_goosrc.py 30")
os.system("echo y | python mongoo.py config=config_test reset")
os.system("python mongoo.py config=config_test init")
os.system("python mongoo.py config=config_test process > log1 2>&1 &")
os.system("python mongoo.py config=config_test process > log2 2>&1")
print "----LOG1----"
os.system("cat log1")
print
print "----LOG2----"
os.system("cat log2")
print
print "------------"
os.system("echo y | python make_goosrc.py 50")
os.system("python mongoo.py config=config_test init")
os.system("python mongoo.py config=config_test process > log3 2>&1 &")
os.system("python mongoo.py config=config_test process > log4 2>&1")
print
print "----LOG3----"
os.system("cat log3")
print
print "----LOG4----"
os.system("cat log4")
print
print
print
os.system("python mongoo.py config=config_test status")