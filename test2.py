#!/usr/bin/python
import os, time
os.system("rm log1 log2 log3 log4 log5 log6")
os.system("echo y | python mongoo.py config=config_test2 reset")
os.system("python mongoo.py config=config_test2 init")
os.system("python mongoo.py config=config_test2 process > log1 2>&1 &")
os.system("python mongoo.py config=config_test2 process > log2 2>&1 &")
os.system("python mongoo.py config=config_test2 process > log3 2>&1 &")
os.system("python mongoo.py config=config_test2 process > log4 2>&1 &")
os.system("python mongoo.py config=config_test2 process > log5 2>&1 &")
os.system("python mongoo.py config=config_test2 process > log6 2>&1")
time.sleep(3)           #make sure it's all done
print "----LOG1----"
os.system("cat log1")
print
print "----LOG2----"
os.system("cat log2")
print
print "----LOG3----"
os.system("cat log3")
print
print "----LOG4----"
os.system("cat log4")
print
print
print
os.system("python mongoo.py config=config_test2 status")
os.system("grep seconds log1 log2 log3 log4 log5 log6")