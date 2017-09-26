import unittest, copy
import os
import time

from pyArango.connection import *
from pyArango.database import *
from pyArango.collection import *

# A little script to test performaces

def createUsers(collection, i) :
    doc = collection.createDocument()
    doc["name"] = "Tesla-%d" % i
    doc["number"] = i
    doc["species"] = "human"
    doc.save()

conn = Connection(username="root", password="root")
# conn = Connection(username=None, password=None)

print ("Creating db...")
try :
    db = conn.createDatabase(name = "test_db_2")
except :
    print ("DB already exists")
db = conn["test_db_2"]

try :
    collection = db.createCollection(name = "users")
except :
    print ("Collection already exists")

collection = db["users"]
collection.truncate()

startTime = time.time()
nbUsers = 1000000

for i in range(nbUsers) :
    if i % 1000 == 0 :
        print ("->", i, "saved")
    try :
        createUsers(collection, i)
    except Exception as e:
        print ("died at", i)
        raise e

took = time.time() - startTime
print ("avg, 1sc => ", float(nbUsers)/took, "saves")

print ("Cleaning up...")

db["users"].delete()
collection.truncate()
print ("Done...")