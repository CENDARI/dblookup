This python project allows to build an index entry in ElasticSearch containing all the entities useful somewhow to the Cendari project.

## Installation

Python 2.7 is required, as well as bzip2 and wget. From python, pip needs to be installed, as well as Fabric and virtualenv.

To create the environment, type:
   ```
   fab setup
   ```

To download all the dbpedia dump files, type:
   ```
   fab download_dbpedia
   ```

This should take time (one hour on a good network) and space (1.5Gb).

To compute the index file, type:
   ```
   fab create_index
   ```
It should create a large compressed file called `dbpedia-<date>.json.bz2` in around one hour depending on your machine.

To send it to elasticsearch, use the shell script:
   ```
   ./big_bulk_index.sh dbpedia-<date>.json.bz2
   ```

It will create a directory called `split` in the current directory (it should be in /tmp I guess), split the dump file in 1000 lines chunks, and send them all to elasticsearch on localhost.
Configure the script of you want to change the index or host to send it to.

In the end, the `split` directory is kept for inspection. For all the files with strange names (e.g. `xzcyg`), there is the reply from elasticsearch names `abc.out`. The first thing visible in it is the error condition, which should be `"errors":false`.

Once you have inspected the files, you can get rid of the directory:
     ```
     rm -rf split
     ```

Keep the json dump file if you want to reinstall everything after a crash. Otherwise, it will take time to rebuild.
