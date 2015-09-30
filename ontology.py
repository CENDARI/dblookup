from rdflib.graph import Graph, URIRef, Literal
from rdflib.namespace import Namespace, NamespaceManager, RDF, RDFS, OWL, XSD
from rdflib.plugins.parsers.nt import *
from rdflib.plugins.parsers.ntriples import NTriplesParser, ParseError, r_wspace, r_wspaces, r_tail
import json
from bz2file import BZ2File
import csv
import pprint
import datetime
import sys, os

DBOWL   = Namespace("http://dbpedia.org/ontology/")
SCHEMA  = Namespace("http://schema.org/")
GRS     = Namespace("http://www.georss.org/georss/")
DBPPROP  = Namespace("http://dbpedia.org/property/")
GEO     = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

CLASS = OWL['Class']
SUBCLASSOF = RDFS['subClassOf']
EQUIVALENT = OWL['equivalentClass']
TYPE = RDF.type

type_class = set()
type_subclass = {}
type_equiv = {}

class MyNTriplesParser(NTriplesParser):
    def parseline(self, line):
        self.line = line
        self.eat(r_wspace)
        if (not self.line) or self.line.startswith('#'):
            return (None, None, None)

        subject = self.subject()
        self.eat(r_wspaces)

        predicate = self.predicate()
        self.eat(r_wspaces)

        object = self.object()
        self.eat(r_tail)

        return (subject, predicate, object)

parser = MyNTriplesParser()

def add_subclass(uri, cl):
    global type_subclass
    s = type_subclass.get(uri)
    if s is None:
        s = {cl}
        type_subclass[uri] = s
    else:
        s.add(cl)

def add_equiv(uri, cl):
    global type_equiv
    uri = str(uri)
    s = type_equiv.get(uri)
    if s is None:
        s = {cl}
        type_equiv[uri] = s
    else:
        s.add(cl)

def load_ontology(filename):
    global type_class
    with BZ2File(filename) as f:
        for line in f:
            try:
                s, p, o = parser.parseline(line)
                if o==CLASS:
                    type_class.add(str(s))
                elif p==SUBCLASSOF:
                    add_subclass(str(s), str(o))
                elif p==EQUIVALENT:
                    add_equiv(str(s), str(o))
            except:
                exctype, value = sys.exc_info()[:2]
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)


type_transitive = {}
    
def transitive(uri):
    if uri in type_transitive:
        return type_transitive[uri]
    type_transitive[uri] = set()
    trans = {uri}
    if uri in type_equiv:
        trans |= type_equiv[uri]
    if uri in type_subclass:
        for t in type_subclass[uri]:
            trans |= transitive(t)
    type_transitive[uri] = trans
    return trans

def compute_transitive():
    for uri in type_class.keys():
        transitive(uri)

def is_a(uri,types):
    uri = str(uri)
    if uri not in type_class:
        return False
    if not isinstance(types, list):
        types = [types]
    for t in types:
        t = str(t)
        if t in transitive(uri):
            return t
    return False

if __name__=="__main__":
    load_ontology('dbpedia/dbpedia_2015-04.nt.bz2')
    l = list(type_class)
    l.sort()
    for uri in l:
        s = transitive(uri)
        print '%s: %s' %(uri, list(s))
