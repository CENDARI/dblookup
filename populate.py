#!/usr/bin/env python
from pyelasticsearch import ElasticSearch
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


DEBUG = False


DBOWL   = Namespace("http://dbpedia.org/ontology/")
SCHEMA  = Namespace("http://schema.org/")
GRS     = Namespace("http://www.georss.org/georss/")

GRSPOINT = GRS['point']
LABEL =   URIRef('http://www.w3.org/2000/01/rdf-schema#label')
COMMENT = URIRef('http://www.w3.org/2000/01/rdf-schema#comment')

NOW = datetime.datetime.now()
TODAY = datetime.date.today()
ORIGIN = datetime.date(1, 1, 1)
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
    'http://schema.org/Event': 'event',
    'http://schema.org/Organization': 'org',
    'http://schema.org/Person': 'person',
    'http://schema.org/CreativeWork': 'ref',
    'http://schema.org/Place': 'place'
}

def get_entry(url):
    url = unicode(url)
    if url in entries:
        entry = entries[url]
    else:
        entry = {
            'uri': url,
            'fetched': NOW,
            'application': 'dbpedia',
            'project': 'dbpedia',
            'language': 'en'}
        entries[url] = entry
    return entry

def add_to_entry(entry, key, value):
    if key in entry:
        v = entry[key]
        if isinstance(v, set):
            v.add(value)
        else:
            if value != v:
                entry[key] = set([v, value])
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
                        add_to_entry(entry, 'title', o)
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
                        entry['text'] = unicode(o)
                        update()
            except:
                exctype, value = sys.exc_info()[:2]
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)

            if DEBUG and (updates-su) >= DEBUG: break

    print 'Updated %d records' % (updates - su)
    su = updates

    if os.path.exists('dbpedia/raw_infobox_properties_en.nt.bz2'):
        print 'Loading infobox properties'
        with BZ2File('dbpedia/raw_infobox_properties_en.nt.bz2') as f:
            for line in f:
                try:
                    s, p, o = parser.parseline(line)
                    #TODO check well-known date properties?
                    if isinstance(o, Literal) and o.datatype==XSD.date:
                        s = unicode(s)
                        if s in entries:
                            o = o.toPython()
                            if isinstance(o, date) and o <= TODAY: # and o >= ORIGIN:
                                entry = entries[s]
                                add_to_entry(entry, 'date', o)
                                update()
                except:
                    exctype, value = sys.exc_info()[:2]
                    print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)

                if DEBUG and (updates-su) >= DEBUG: break

    print 'Updated %d records' % (updates - su)
    su = updates

    if os.path.exists('dbpedia/wikistats-2015-enf.csv.bz2'):
        print 'Loading wikipedia log stats'
        with BZ2File('dbpedia/wikistats-2015-enf.csv.bz2') as f:
            reader = csv.reader(f)
            for row in reader:
                try:
                    (lang, name, year, _, count) = row
                    s = 'http://dbpedia.org/resource/'+name
                    if s in entries:
                        entry = entries[s]
                        entry['pageviews'] = count
                        update()
                except:
                    exctype, value = sys.exc_info()[:2]
                    print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)

                if DEBUG and (updates-su) >= DEBUG: break

    print 'Updated %d records' % (updates - su)
    su = updates

populate()
print 'Saving...'

es = ElasticSearch('http://localhost:9200/')


def entries_iterator():
    for url, e in entries.iteritems():
        if not url in redirects and 'title' in e:
            if 'class' in e:
                v = e['class']
                if not isinstance(v, set):
                    v = [v]
                for c in v:
                    if c in interesting:
                        facet = interesting[c]
                        e[facet] = e['title']
            yield es.index_op(e, id=url)

with BZ2File(dumpfilename, 'wb') as out:
    for e in entries_iterator():
        out.write(e)
        out.write('\n')
print 'Done.'
