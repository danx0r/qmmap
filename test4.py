#!/usr/bin/python
import os
os.system("rm log1")
os.system("echo y | python make_goosrc4.py config=config_test 30")
os.system("echo y | python mongoo.py config=config_test reset")
os.system("python mongoo.py config=config_test init")
os.system("python mongoo.py config=config_test process > log1 2>&1")
print "----LOG1----"
os.system("cat log1")
print
print
print
os.system("python mongoo.py config=config_test status")