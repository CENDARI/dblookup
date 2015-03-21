curl -XPUT 'http://localhost:9200/cendari/'; echo
curl -XPUT 'http://localhost:9200/cendari/entity/_mapping' -d '@mapping.json'; echo
