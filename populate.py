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
from ontology import is_a, load_ontology, parser

DEBUG = False


DBOWL   = Namespace("http://dbpedia.org/ontology/")
SCHEMA  = Namespace("http://schema.org/")
GRS     = Namespace("http://www.georss.org/georss/")
DBPPROP  = Namespace("http://dbpedia.org/property/")
GEO     = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")

GRSPOINT = GRS['point']
LABEL =   URIRef('http://www.w3.org/2000/01/rdf-schema#label')
COMMENT = URIRef('http://www.w3.org/2000/01/rdf-schema#comment')

NOW = datetime.datetime.now()
TODAY = datetime.date.today()
ORIGIN = datetime.date(1, 1, 1)
timestamp = NOW.strftime('%Y-%m-%dT%H-%M-%S')
dumpfilename = 'dbpedia-%s.json.bz2' % timestamp

print 'Populating DBPedia dump file %s' % dumpfilename

entries = {}
redirects = set()

facets = {
    'http://schema.org/Event': 'event',
    'http://schema.org/Organization': 'org',
    'http://schema.org/Person': 'person',
    'http://schema.org/CreativeWork': 'ref',
    'http://schema.org/Place': 'place'
}

interesting = facets.keys()

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

def update(incr=1):
    global updates
    updates += incr
    if (updates % 10000) == 0:
        print 'Read %d lines, %d entries' % (updates, len(entries))
        #pprint.pprint(entries)

geoloc_props = {
    GRS['point'],
    GEO['geometry'],
    GEO['lat'], GEO['long'],
    DBPPROP['latitude'], DBPPROP['longitude'],
    DBPPROP['latd'], DBPPROP['latm'], DBPPROP['longd'], DBPPROP['lonm'],
    DBPPROP['latDeg'], DBPPROP['latMin'], DBPPROP['lonDeg'], DBPPROP['lonMin']
}

def associate_geo(entry, values, id):
    if entry is None or len(values)==0 or 'location' in entry:
        return None
        
    loc = None
    if GRSPOINT in values:
        o = values.get(GRSPOINT)
        loc = unicode(o)
    while not loc:
        o = values.get(GEO['geometry'])

        match = []
        if o and \
          (match.append(re.match(r"POINT\(([^ ]+) ([^ ]+)\)", unicode(o))) or any(match)):
            loc = match.pop().group(2)+' '+match.pop().group(1)
            break
        lat = values.get(GEO['lat'])
        lon = values.get(GEO['long'])
        if lat and lon:
            loc = str(lat)+" "+str(lon)
            break

        lat = values.get(DBPPROP['latitude'])
        lon = values.get(DBPPROP['longitude'])
        if lat and lon:
            loc = str(lat)+" "+str(lon)
            break

        lat = values.get(DBPPROP['latd'])
        latmin = values.get(DBPPROP['latm'])
        lon = values.get(DBPPROP['longd'])
        lonmin = values.get(DBPPROP['lonm'])
        if lat and latmin and lon and lonmin:
            # Degrees + minutes/60 
            lat = float(lat) + float(latmin)/60.0
            lon = float(lon) + float(lonmin)/60.0
            loc = "%f %f" % (lat, lon)
            loc = str(lat)+" "+str(lon)
            break

        lat = values.get(DBPPROP['latDeg'])
        latmin = values.get(DBPPROP['latMin'])
        lon = values.get(DBPPROP['lonDeg'])
        lonmin = values.get(DBPPROP['lonMin'])
        if lat and latmin and lon and lonmin:
            # Degrees + minutes/60 
            lat = float(lat) + float(latmin)/60.0
            lon = float(lon) + float(lonmin)/60.0
            loc = "%f %f" % (lat, lon)
            break
        break

    if loc:
        if 'class' in entry:
            add_to_entry(entry, 'class', 'http://schema.org/Place')
        else:
            entry['class'] = 'http://schema.org/Place'
        entry['location'] = loc.replace(' ', ',')
        print 'Found loc for %s: %s' % (id, loc)
    update(len(values))



def populate():
    global updates
    print 'Loading dbpedia ontology'
    load_ontology('dbpedia/dbpedia_2015-04.nt.bz2')
    print 'Loading types'
    with BZ2File('dbpedia/instance-types_en.nt.bz2') as f:
        for line in f:
            try:
                s, p, o = parser.parseline(line)
                if p == RDF.type:
                    o = str(o)
                    t=is_a(o,interesting)
                    if t:
                        entry = get_entry(s)
                        add_to_entry(entry, 'class', t)
                        update()
            except:
                exctype, value = sys.exc_info()[:2]
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)
            if DEBUG and updates >= DEBUG: break

    print 'Loading redirects'
    with BZ2File('dbpedia/transitive-redirects_en.nt.bz2') as f:
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
    last_uri = None
    last_entry = None
    geo_props = {}
    with BZ2File('dbpedia/geo-coordinates_en.nt.bz2') as f:
        for line in f:
            try:
                s, p, o = parser.parseline(line)
                if s!=last_uri:
                    associate_geo(last_entry,geo_props, last_uri)
                    last_uri = s
                    geo_props = {}
                    last_entry = get_entry(s)
                if not last_entry:
                    continue # skip that line
                geo_props[p] = o
            except:
                exctype, value = sys.exc_info()[:2]
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)
            if len(geo_props):
                associate_geo(last_entry, geo_props, last_uri)
                last_uri = None
                last_entry = None
                geo_props = {}
                if DEBUG and (updates-su) >= DEBUG: break

    print 'Updated %d records' % (updates - su)
    su = updates

    print 'Loading infobox properties'
    with BZ2File('dbpedia/infobox-properties_en.nt.bz2') as f:
        for line in f:
            try:
                s, p, o = parser.parseline(line)
                if s!=last_uri:
                    associate_geo(last_entry,geo_props, last_uri)
                    last_uri = s
                    geo_props = {}
                    last_entry = get_entry(s)
                if not last_entry:
                    continue # skip that line
                #TODO check well-known date properties?
                if isinstance(o, Literal) and o.datatype==XSD.date:
                    o = o.toPython()
                    add_to_entry(last_entry, 'date', o)
                    update()
                elif p in geoloc_props:
                    geo_props[p] = o
            except:
                exctype, value = sys.exc_info()[:2]
                print >>sys.stderr, 'Exception: %s(%s)' % (exctype,value)

            if len(geo_props):
                associate_geo(last_entry, geo_props, last_uri)
            if DEBUG and (updates-su) >= DEBUG: break

    print 'Updated %d records' % (updates - su)

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
    with BZ2File('dbpedia/short-abstracts_en.nt.bz2') as f:
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
                    if c in facets:
                        facet = facets[c]
                        e[facet] = e['title']
            yield es.index_op(e, id=url)

with BZ2File(dumpfilename, 'wb') as out:
    for e in entries_iterator():
        out.write(e)
        out.write('\n')
print 'Done.'
