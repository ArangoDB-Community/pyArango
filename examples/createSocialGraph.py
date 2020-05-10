#!/usr/bin/python
import sys
from pyArango.connection import *
from pyArango.graph import *
from pyArango.collection import *


class Social(object):
        class male(Collection):
            _fields = {
                "name" : Field()
            }
            
        class female(Collection):
            _fields = {
                "name" : Field()
            }
            
        class relation(Edges):
            _fields = {
                "number" : Field()
            }
            
        class social(Graph):

            _edgeDefinitions = (EdgeDefinition ('relation',
                                                fromCollections = ["female", "male"],
                                                toCollections = ["female", "male"]),)
            _orphanedCollections = []


        def __init__(self):
               self.conn = Connection(username="USERNAME", password="SECRET")
        
               self.db = self.conn["_system"]
               if self.db.hasGraph('social'):
                   raise Exception("The social graph was already provisioned! remove it first")

               self.female   = self.db.createCollection(className='Collection', name='female')
               self.male     = self.db.createCollection(className='Collection', name='male')

               self.relation = self.db.createCollection(className='Edges', name='relation')

               g = self.db.createGraph("social")
               
               a = g.createVertex('female', {"name": 'Alice',  "_key": 'alice'});
               b = g.createVertex('male',  {"name": 'Bob',    "_key": 'bob'});
               c = g.createVertex('male',   {"name": 'Charly', "_key": 'charly'});
               d = g.createVertex('female', {"name": 'Diana',  "_key": 'diana'});
               a.save()
               b.save()
               c.save()
               d.save()

               g.link('relation', a, b, {"type": 'married', "_key": 'aliceAndBob'})
               g.link('relation', a, c, {"type": 'friend', "_key": 'aliceAndCharly'})
               g.link('relation', c, d, {"type": 'married', "_key": 'charlyAndDiana'})
               g.link('relation', b, d, {"type": 'friend', "_key": 'bobAndDiana'})


Social()
