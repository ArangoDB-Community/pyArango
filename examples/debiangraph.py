#/usr/bin/env python3
"""
example to build a file with all packages known to your system for a debian jessie:

for i in $(ls /var/lib/apt/lists/*debian_dists_jessie_* |\
           grep -v i386 |grep -v Release); do
    cat $i >> /tmp/allpackages;
    echo >> /tmp/allpackages;
done

All debian based distros have a set of files in /var/lib/apt/lists.
In doubt create a filter for your distro

Install needed Python modules:

pip3 install pyArango
pip3 install deb_pkg_tools
"""

import deb_pkg_tools
from deb_pkg_tools.control import deb822_from_string
from deb_pkg_tools.control import parse_control_fields
from pyArango.connection import *
from pyArango.database import *
from pyArango.collection import *
from pyArango.document import *
from pyArango.query import *
from pyArango.graph import *
from pyArango.theExceptions import *

# Configure your ArangoDB server connection here
conn = Connection(arangoURL="http://localhost:8529", username="root", password="")

db = None
edgeCols = {}
packagesCol = {}

# we create our own database so we don't interfere with userdata:

if not conn.hasDatabase("testdb"):
    db = conn.createDatabase("testdb")
else:
    db = conn["testdb"]

if not db.hasCollection('packages'):
    packagesCol = db.createCollection('Collection', name='packages')
else:
    packagesCol = db.collections['packages']


def getEdgeCol(name):
    if not name in edgeCols:
        if not db.hasCollection(name):
            edgeCols[name] = db.createCollection(name=name, className='Edges')
        else:
            edgeCols[name] = db.collections[name]
    return edgeCols[name]


def saveGraphDefinition():
    graph_collection = db.collections["_graphs"]
    graph_defintion = {
        "_key": "debian_dependency_graph",
        "edgeDefinitions": [],
        "orphanCollections": [],
    }
    for collection in edgeCols.keys():
        graph_defintion["edgeDefinitions"].append(
            {"collection": collection,
             "from": ["packages",],
             "to": ["packages",]})
    graph_collection.createDocument(graph_defintion).save()


def VersionedDependencyToDict(oneDep, hasAlternatives):
    return {
        'name': oneDep.name,
        'version': oneDep.version,
        'operator': oneDep.operator,
        'hasAlternatives': hasAlternatives
        }


def DependencyToDict(oneDep, hasAlternatives):
    return {
        'name': oneDep.name,
        'hasAlternatives': hasAlternatives
        }


def DependencySetToDict(dep, hasAlternatives):
    depset = []
    for oneDep in dep.relationships:
        if isinstance(oneDep, deb_pkg_tools.deps.VersionedRelationship):
            depset.append(VersionedDependencyToDict(oneDep, hasAlternatives))
        elif isinstance(oneDep, deb_pkg_tools.deps.AlternativeRelationship):
            depset.append(DependencySetToDict(oneDep, True))
        elif isinstance(oneDep, deb_pkg_tools.deps.Relationship):
            depset.append(DependencyToDict(oneDep, hasAlternatives))
        else:
            print("Unknown relationshitp: " + repr(oneDep))
    return depset


def PackageToDict(pkg):
    # packages aren't serializable by default, translate it:
    ret = {}
    for attribute in pkg.keys():
        if isinstance(pkg[attribute], deb_pkg_tools.deps.RelationshipSet):
            # relation ship field to become an array of relations:
            ret[attribute] = DependencySetToDict(pkg[attribute], False)
        else:
            # regular string field:
            ret[attribute] = pkg[attribute]
    ret["_key"] = ret["Package"]
    return ret


def saveDependencyToEdgeCol(edgeCol, dep, pname, hasAlternatives):
    for oneDep in dep.relationships:
        if isinstance(oneDep, deb_pkg_tools.deps.VersionedRelationship):
            # version dependend relations:
            d = VersionedDependencyToDict(oneDep, hasAlternatives)
            d['_from'] = 'packages/' + pname
            d['_to'] = 'packages/' + oneDep.name
            relation = edgeCol.createDocument(d).save()
        elif isinstance(oneDep, deb_pkg_tools.deps.AlternativeRelationship):
            # A set of alternative relations; recurse:
            saveDependencyToEdgeCol(edgeCol, oneDep, pname, True)
        elif isinstance(oneDep, deb_pkg_tools.deps.Relationship):
            # simple relations only to package names without versions:
            d = DependencyToDict(oneDep, hasAlternatives)
            d['_from'] = 'packages/' + pname
            d['_to'] = 'packages/' + oneDep.name
            relation = edgeCol.createDocument(d).save()
        else:
            print("Unknown relationshitp: " + repr(oneDep))

#
# Main import routine
#

onePackage = ''
for line in open('/tmp/allpackages', encoding='utf-8'):
    # Package blocks are separated by new lines.
    if len(line) == 1 and len(onePackage) > 4:
        pkg = deb822_from_string(onePackage)
        pname = pkg['Package']
        pkg1 = parse_control_fields(pkg)
        p = PackageToDict(pkg1)
        try:
            packagesCol.createDocument(p).save()
            for key in pkg1.keys():
                # filter for fields with relations:
                if isinstance(pkg1[key], deb_pkg_tools.deps.RelationshipSet):
                    # save one relation set to field:
                    saveDependencyToEdgeCol(getEdgeCol(key), pkg1[key], pname, False)
            onePackage = ''
        except CreationError:
            pass
    else:
        onePackage += line

saveGraphDefinition()
