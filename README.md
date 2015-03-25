# hpg-bigdata
This repository implements converters and tools for working with NGS data in HPC or Hadoop cluster

DOWNLOAD and BUILDING
---------------------

    $ git clone https://github.com/opencb/hpg-bigdata.git
    $ mvn install

RUNING
-------

  For command line options invoke:

    $ ./hpg-bigdata.sh -h



ga4gh command
-------------

  To save a Fastq file as an Avro file according to GA4GH schemas (conversion fastq2ga):
   
    $ ./hpg-bigdata.sh ga4gh -c fastq2ga -i test1.fq -o test1.fq.ga -x snappy
    $ ./hpg-bigdata.sh ga4gh -c fastq2ga -i test1.fq -o hdfs://test1.fq.ga -x snappy

  The previous Avro files can be saved back in Fastq format with the commands (conversion ga2fastq):
   
    $ ./hpg-bigdata.sh ga4gh -c ga2fastq -i test1.fq.ga -o test2.fq
    $ ./hpg-bigdata.sh ga4gh -c ga2fastq -i hdfs://test1.fq.ga -o test3.fq 
    
   