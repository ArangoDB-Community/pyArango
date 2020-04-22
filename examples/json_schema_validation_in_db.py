#!/usr/bin/env python3
import sys
import time
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *

def main():
    conn = Connection(username="", password="")
    db = conn["_system"]
    name = "pyArangoValidation"

    schema = {
        "rule" : {
            "properties" : {
                "value" : {
                    "type" : "number"
                }
            }
        },
        "level" : "strict",
        "message" : "invalid document - schema validation failed!"
    }

    collection = None
    if db.hasCollection(name):
        db[name].delete() # drop
        db.reloadCollections() # work around drop should reload...

    collection = db.createCollection(
        name = name,
        schema = schema
    )

    try:
        d = collection.createDocument()
        d["value"] = "bar"
        d.save()
    except Exception as e:
        print(e)

    d = collection.createDocument()
    d["value"] = 3
    d.save()

    print(collection.fetchAll())
    return 0

if __name__ == "__main__":
    sys.exit(main())
