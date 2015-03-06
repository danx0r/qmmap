#!/usr/bin/python
import os
os.system("python mongoo.py reset")
os.system("python mongoo.py init")
os.system("python mongoo.py process > log1 &")
os.system("python mongoo.py process | tee log2")
