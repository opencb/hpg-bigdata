# Overview
This repository implements converters and tools for working with NGS data in HPC or Hadoop cluster


### Download and Building


    $ git clone https://github.com/opencb/hpg-bigdata.git
    $ ./build.sh

##### Requirements

  1. Java 1.8
  2. opencb/java-common-libs v3.0-SNAPSHOT
  3. cmake
  4. Libraries: libz, libsnappy, liblzma, libncurses

### Running


  For command line options invoke:

```bash

    ./hpg-bigdata.sh -h

```

##### _convert_ command
The command **convert** al lows you to save Fastq, SAM, BAM, VCF,... files as Avro files according to the GA4GH models. You can specify a compression method, e.g., deflate, snappy, bzip2.
The source files (Fastq, SAM, BAM...) have to be located in the local file system, on the other hand, destination files can be saved both in the local file system and in the Hadoop file system (HDFS), in the latter case, you must use the notation **hdfs://**
  
  Some examples using the test files in the folder data:
   
    $ ./hpg-bigdata.sh convert -c fastq2ga -i data/test.fq -o data/test.fq.ga -x snappy
    $ ./hpg-bigdata.sh convert -c fastq2ga -i data/test.fq -o hdfs://test.fq.hdfs.ga -x snappy
    
    $ ./hpg-bigdata.sh convert -c sam2ga -i data/test.sam -o data/test.sam.ga -x deflate
    $ ./hpg-bigdata.sh convert -c sam2ga -i data/test.sam -o hdfs://test.sam.hdfs.ga -x deflate
    
    $ ./hpg-bigdata.sh convert -c bam2ga -i data/test.bam -o data/test.bam.ga -x bzip2
    $ ./hpg-bigdata.sh convert -c bam2ga -i data/test.bam -o hdfs://test.bam.hdfs.ga -x bzip2

    $ ./hpg-bigdata.sh convert -c vcf2ga -i data/test.vcf.gz -o test.vcf.ga.avro -x snappy
    $ ./hpg-bigdata.sh convert -c vcf2ga -i data/test.vcf.gz -o hdfs://test.vcf.gz.hdfs.ga.avro -x snappy

  In addition, by using the command **convert**, you can save the Avro files as the original formats (Fastq, SAM, BAM...). In this case, the Avro files can be located both in the local file system and in the HDFS. 
  
  Some examples:
   
    $ ./hpg-bigdata.sh convert -c ga2fastq -i data/test.fq.ga -o data/test.fq.ga.fq
    $ ./hpg-bigdata.sh convert -c ga2fastq -i hdfs://test.fq.hdfs.ga -o data/test.fq.hdfs.ga.fq 
    
    $ ./hpg-bigdata.sh convert -c ga2sam -i data/test.sam.ga -o data/test.sam.ga.sam
    $ ./hpg-bigdata.sh convert -c ga2sam -i hdfs://test.sam.hdfs.ga -o data/test.sam.hdfs.ga.sam
    
    $ ./hpg-bigdata.sh convert -c ga2bam -i data/test.bam.ga -o data/test.sam.ga.bam
    $ ./hpg-bigdata.sh convert -c ga2bam -i hdfs://test.bam.hdfs.ga -o data/test.bam.hdfs.ga.bam
   

##### _fastq_ command
  The command **fastq** allows you to compute some statistical values for a given Fastq file that must be stored in the Haddop/HDFS environment, and according to the GA4GH Avro models (Check the command **convert**)
  
  Some examples:
   
    $ ./hpg-bigdata.sh fastq --stats -i hdfs://test.fq.ga -o /tmp/stats-test.fq
    $ ./hpg-bigdata.sh fastq --kmers 7 -i hdfs://test.fq.ga -o /tmp/kmers-test.fq
    $ ./hpg-bigdata.sh fastq --stats --kmers 7 -i hdfs://test.fq.ga -o /tmp/stats-kmers-test.fq
    
