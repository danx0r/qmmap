#!/usr/bin/python
import os
os.system("rm log1")
os.system("echo y | python make_goosrc4.py mongodb://127.0.0.1/local_db 30")
os.system("echo y | python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest reset")
os.system("python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest init")
os.system("python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest process > log1 2>&1 &")
os.system("python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest process > log2 2>&1 &")
os.system("python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest wait --timeout 5")
os.system("python mongoo.py mongodb://127.0.0.1/local_db goosrc . goodest status")