import unittest, copy
import os
import time
import random

from pyArango.connection import *
from pyArango.database import *
from pyArango.collection import *

from pyArango.collection import BulkOperation as BulkOperation

# A little script to test performaces

allUsers = []

def createUsers(collection, i):
    global allUsers
    doc = collection.createDocument()
    doc["name"] = "Tesla-%d" % i
    doc["number"] = i
    doc["species"] = "human"
    allUsers.append(doc)
    doc.save()

allLinks = []
def linkUsers(collection, userA, userB, count):
    global allLinks
    doc = collection.createEdge()
    doc["count"] = count
    doc.links(userA, userB)
    allLinks.append(doc)
    
conn = Connection(username="root", password="")
# conn = Connection(username=None, password=None)

print ("Creating db...")
try:
    db = conn.createDatabase(name = "test_db_2")
except:
    print ("DB already exists")
db = conn["test_db_2"]

try:
    collection = db.createCollection(name = "users")
except:
    print ("Collection already exists")

try:
    relcol = db.createCollection(className = 'Edges', name = "relations")
except:
    print ("Relations Collection already exists")

collection = db["users"]
collection.truncate()

relcol = db["relations"]
relcol.truncate()

startTime = time.time()
nbUsers = 100000
batchSize = 500

print("Saving Users: ")
with BulkOperation(collection, batchSize=batchSize) as col:
    for i in range(nbUsers):
        if i % 1000 == 0:
            print ("->", i, "saved")
        try:
            createUsers(col, i)
        except Exception as e:
            print ("died at", i)
            raise e

i = 0
with BulkOperation(relcol, batchSize=batchSize) as col:
    for userA in allUsers:
        i += 1
        try:
            otherUser = random.choice(allUsers)
            linkUsers(col, userA, otherUser, i)
        except Exception as e:
            print ("died at", userA)
            raise e

print("Modifying relations: \n")
with BulkOperation(relcol, batchSize=batchSize) as col:
    for link in allLinks:
        try:
            link.set({'modified': 'true'})
            link.patch()
        except Exception as e:
            print ("died at", link)
            raise e

print("Modifying Users: \n")
with BulkOperation(collection, batchSize=batchSize) as col:
    for user in allUsers:
        try:
            user.set({'modified': 'true'})
            user.patch()
        except Exception as e:
            print ("died at", link)
            raise e

print("Deleting relations: \n")
with BulkOperation(relcol, batchSize=batchSize) as col:
    for link in allLinks:
        try:
            link.delete()
        except Exception as e:
            print ("died at", link)
            raise e

print("Deleting Users: \n")
with BulkOperation(collection, batchSize=batchSize) as col:
    for user in allUsers:
        try:
            user.delete()
        except Exception as e:
            print ("died at", link)
            raise e

            
took = time.time() - startTime
print ("avg, 1sc => ", float(nbUsers)/took, "saves")

print ("Cleaning up...")

db["users"].delete()
collection.truncate()
print ("Done...")
