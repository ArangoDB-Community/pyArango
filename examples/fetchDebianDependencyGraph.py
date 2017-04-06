#!/usr/bin/python
import sys
from pyArango.connection import *
from pyArango.graph import *
from asciitree import *

conn = Connection(username="USERNAME", password="SECRET")

db = conn["ddependencyGrahp"]

if not db.hasGraph('debian_dependency_graph'):
    raise Exception("didn't find the debian dependency graph, please import first!")

ddGraph = db.graphs['debian_dependency_graph']

graphQuery = '''
FOR package, depends, path IN
    1..2 ANY
     @startPackage Depends RETURN path
'''

startNode = sys.argv[1]

bindVars =  { "startPackage": "packages/" + startNode }

queryResult = db.AQLQuery(graphQuery, bindVars=bindVars, rawResults=True)

# sub iterateable object to build up the tree for draw_tree:
class Node(object):
    def __init__(self, name, children):
        self.name = name
        self.children = children
        
    def getChild(self, searchName):
        for child in self.children:
            if child.name == searchName:
                return child
        return None
    
    def __str__(self):
        return self.name

def iteratePath(path, depth, currentNode):
    pname = path[depth]['name']
    subNode = currentNode.getChild(pname)
    if subNode == None:
        subNode = Node(pname, [])
        currentNode.children.append(subNode)
    if len(path) > depth + 1:
        iteratePath(path, depth + 1, subNode)

# Now we fold the paths substructure into the tree:
rootNode = Node(startNode, [])
for path in queryResult:
    p = path['edges']
    iteratePath(p, 0, rootNode)

print draw_tree(rootNode)
