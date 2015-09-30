from fabric.api import env, local, lcd
from fabric.colors import red, green
from fabric.decorators import task, runs_once
from fabric.operations import prompt
from fabric.utils import abort
from zipfile import ZipFile

import datetime
import fileinput
import importlib
import os
import random
import re
import subprocess
import sys
import time

PROJ_ROOT = os.path.dirname(env.real_fabfile)
env.project_name = 'dblookup'
env.python = 'python' if 'VIRTUAL_ENV' in os.environ else './bin/python'

@task
def setup():
    """
    Set up a local development environment

    This command must be run with Fabric installed globally (not inside a
    virtual environment)
    """
    if os.getenv('VIRTUAL_ENV') or hasattr(sys, 'real_prefix'):
        abort(red('Deactivate any virtual environments before continuing.'))
    make_virtual_env()
    print ('\nDevelopment environment successfully created.')


@task
def download_dbpedia():
    "Download files from dbpedia"
    with lcd(PROJ_ROOT):
        local('if [ ! -d dbpedia-2015 ]; then mkdir dbpedia-2015; fi')
        server = 'http://downloads.dbpedia.org/2015-04/core'
        #server = 'http://data.dws.informatik.uni-mannheim.de/dbpedia/2014/en/'
        files = [
            'dbpedia_2015-04.nt.bz2',
            'instance-types_en.nt.bz2',
            'labels_en.nt.bz2',
            'transitive-redirects_en.nt.bz2',
            'short-abstracts_en.nt.bz2',
            'geo-coordinates_en.nt.bz2',
            'infobox-properties_en.nt.bz2'
        ]
        with lcd('./dbpedia-2015'):
            for file in files:
                local('wget -N "%s/%s"; fi' % (server, file))
            local('wget -N http://wikistats.ins.cwi.nl/data/wikistats-2015-enf.csv.bz2')

def make_virtual_env():
    "Make a virtual environment for local dev use"
    with lcd(PROJ_ROOT):
        local('virtualenv .')
        local('./bin/pip install -r requirements.txt')

@task
def create_index():
    "Compute a large index containing the dbpedia entries ready to send to ElasticSearch"
    with lcd(PROJ_ROOT):
        local('{python} populate.py'.format(**env))
