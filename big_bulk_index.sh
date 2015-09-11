#!/bin/sh

set -x

rm -rf /tmp/split
mkdir /tmp/split

if expr match "$1" '.*.bz2' > /dev/null; then
    bzcat $1 | (cd /tmp/split; split -a 5)
else
    cat $1 | (cd /tmp/split; split -a 5)
fi

cd /tmp/split
for f in *;
do
    curl -s -XPOST localhost:9200/cendari/entity/_bulk --data-binary @$f > $f.out
done

