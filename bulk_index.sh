#!/bin/sh

if expr match "$1" '.*.bz2' > /dev/null; then
    bunzip2 < $1 | curl -s -XPOST localhost:9200/cendari/entity/_bulk --data-binary @-
else
    curl -s -XPOST localhost:9200/cendari/entity/_bulk --data-binary @$1
fi
