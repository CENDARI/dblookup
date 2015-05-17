#!/bin/sh

#set -x

rm -rf split
mkdir split

cd split
if expr match "$1" '.*.bz2' > /dev/null; then
    bunzip2 < ../$1 | split
else
    split ../$1
fi

for f in *;
do
    curl -s -XPOST localhost:9200/cendari/entity/_bulk --data-binary @$f > $f.out
done

