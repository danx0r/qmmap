#!/usr/bin/python
import os
os.system("rm log1 log2")
os.system("echo y | python mongoo.py config=config_test2 reset")
os.system("python mongoo.py config=config_test2 init")
os.system("python mongoo.py config=config_test2 process > log1 2>&1 &")
os.system("python mongoo.py config=config_test2 process > log2 2>&1")
print "----LOG1----"
os.system("cat log1")
print
print "----LOG2----"
os.system("cat log2")
print
print
print
os.system("python mongoo.py config=config_test2 status")
os.system("grep seconds log1 log2")