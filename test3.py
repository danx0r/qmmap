#!/usr/bin/python
import os, time
os.system("rm log1 log2 log3 log4 log5")
os.system("echo y | python mongoo.py config=config_test3 reset")
os.system("python mongoo.py config=config_test3 init")
os.system("python mongoo.py config=config_test3 process > log1 2>&1 &")
os.system("python mongoo.py config=config_test3 process > log2 2>&1 &")
os.system("python mongoo.py config=config_test3 process > log3 2>&1 &")
os.system("python mongoo.py config=config_test3 process > log4 2>&1 &")
os.system("python mongoo.py config=config_test3 process > log5 2>&1")
time.sleep(30)           #make sure it's all done
print
print
os.system("python mongoo.py config=config_test3 status")
os.system("grep seconds log1 log2 log3 log4 log5")
