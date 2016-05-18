#!/usr/bin/python
import os
os.system("echo y | python make_goosrc.py mongodb://127.0.0.1/local_db 30")
os.system("echo y | python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest reset")
os.system("python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest init")
os.system("python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest process &")
os.system("python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest manage")
