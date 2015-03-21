#!/usr/bin/python
from pyelasticsearch import ElasticSearch
from rdflib.graph import Graph, URIRef
from rdflib.namespace import Namespace, NamespaceManager, RDF, RDFS, OWL
from rdflib.plugins.parsers.nt import *
from rdflib.plugins.parsers.ntriples import NTriplesParser, ParseError, r_wspace, r_wspaces, r_tail
import json
from bz2file import BZ2File
import pprint
import datetime
import sys
import cPickle as pickle

DEBUG = False


DBOWL   = Namespace("http://dbpedia.org/ontology/")
SCHEMA  = Namespace("http://schema.org/")
GRS     = Namespace("http://www.georss.org/georss/")

GRSPOINT = GRS['point']
LABEL =   URIRef('http://www.w3.org/2000/01/rdf-schema#label')
COMMENT = URIRef('http://www.w3.org/2000/01/rdf-schema#comment')

NOW = datetime.datetime.now()
timestamp = NOW.strftime('%Y-%m-%dT%H-%M-%S')
dumpfilename = 'dbpedia-%s.json.bz2' % timestamp

print 'Populating DBPedia dump file %s' % dumpfilename

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

g = Graph()

parser = MyNTriplesParser()

entries = {}
redirects = set()

interesting = {
    'http://schema.org/Event': 'EVT',
    'http://schema.org/Organization': 'ORG',
    'http://schema.org/Person': 'PER',
    'http://schema.org/CreativeWork': 'PUB',
    'http://schema.org/Place': 'PLA'
}


def get_entry(url):
    url = unicode(url)
    if url in entries:
        entry = entries[url]
    else:
        entry = { 'uri': url, 'fetched': NOW }
        entries[url] = entry
    return entry

def add_to_entry(entry, key, value):
    if key in entry:
        v = entry[key]
        if isinstance(v, list):
            v.append(value)
        else:
            entry[key] = [v, value]
    else:
        entry[key] = value

updates = 0

def update():
    global updates
    updates += 1
    if (updates % 10000) == 0:
        print 'Read %d lines, %d entries' % (updates, len(entries))
        #pprint.pprint(entries)

def populate():
    global updates
    print 'Loading types'
    with BZ2File('dbpedia/instance_types_en.nt.bz2') as f:
        for line in f:
            try:
                s, p, o = parser.parseline(line)
                if p == RDF.type:
                    o = unicode(o)
                    if o in interesting:
                        entry = get_entry(s)
                        add_to_entry(entry, 'class', o)
                        update()
            except:
                exctype, value = sys.exc_info()[:2] 
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)
            if DEBUG and updates >= DEBUG: break

    print 'Loading redirects'
    with BZ2File('dbpedia/redirects_en.nt.bz2') as f:
        for line in f:
            try:
                s, p, o = parser.parseline(line)
                o = unicode(o)
                if o in entries:
                    entry = entries[o]
                    s = unicode(s)
                    if s in entries:
                        print 'Clash for %s and %s' % (o, s)
                    else:
                        entries[s] = entry
                        redirects.add(s)
                        update()
            except:
                exctype, value = sys.exc_info()[:2] 
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)
            if DEBUG and updates >= DEBUG: break

    print 'Loading coordinates'
    su = updates
    with BZ2File('dbpedia/geo_coordinates_en.nt.bz2') as f:
        for line in f:
            try:
                s, p, o = parser.parseline(line)
                if p == GRSPOINT:
                    entry = get_entry(s)
                    if 'class' in entry:
                        add_to_entry(entry, 'class', 'http://schema.org/Place')
                    else:
                        entry['class'] = 'http://schema.org/Place'
                    entry['location'] = unicode(o).replace(' ', ',')
                    update()
            except:
                exctype, value = sys.exc_info()[:2] 
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)
            if DEBUG and (updates-su) >= DEBUG: break

    print 'Updated %d records' % (updates - su)
    su = updates

    print 'Loading labels'
    with BZ2File('dbpedia/labels_en.nt.bz2') as f:
        for line in f:
            try:
                s, p, o = parser.parseline(line)
                if p == LABEL:
                    s = unicode(s)
                    if s in entries:
                        o = unicode(o)
                        entry = entries[s]
                        add_to_entry(entry, 'label', o)
                        update()
            except:
                exctype, value = sys.exc_info()[:2] 
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)
            if DEBUG and (updates-su) >= DEBUG: break

    print 'Updated %d records' % (updates - su)
    su = updates

    print 'Loading short abstracts'
    with BZ2File('dbpedia/short_abstracts_en.nt.bz2') as f:
        for line in f:
            try:
                s, p, o = parser.parseline(line)
                if p == COMMENT:
                    s = unicode(s)
                    if s in entries:
                        entry = entries[s]
                        entry['abstract'] = unicode(o)
                        update()
            except:
                exctype, value = sys.exc_info()[:2] 
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)

            if DEBUG and (updates-su) >= DEBUG: break

    print 'Updated %d records' % (updates - su)
    su = updates

populate()
print 'Done'

es = ElasticSearch('http://localhost:9200/')

def entries_iterator():
    for url, e in entries.iteritems():
        if not url in redirects:
            yield es.index_op(e, id=url)

with BZ2File(dumpfilename, 'wb') as out:
    for e in entries_iterator():
        out.write(e)
        out.write('\n')
