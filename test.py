#!/usr/bin/python
import os
os.system("python mongoo.py reset")
os.system("python mongoo.py init")
os.system("python mongoo.py process > log1 2>&1 &")
os.system("python mongoo.py process > log2 2>&1")
os.system("cat log1")
os.system("cat log2")
