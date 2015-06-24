# Overview
This repository implements converters and tools for working with NGS data in HPC or Hadoop cluster

### Documentation
You can find HPG BigData documentation and tutorials at: https://github.com/opencb/hpg-bigdata/wiki.

### Issues Tracking
You can report bugs or request new features at [GitHub issue tracking](https://github.com/opencb/hpg-bigdata/issues).

### Release Notes and Roadmap
Releases notes are available at [GitHub releases](https://github.com/opencb/hpg-bigdata/releases).

Roadmap is available at [GitHub milestones](https://github.com/opencb/hpg-bigdata/milestones). You can report bugs or request new features at [GitHub issue tracking](https://github.com/opencb/hpg-bigdata/issues).

### Versioning
HPG BigData is versioned following the rules from [Semantic versioning](http://semver.org/).

### Maintainers
We recommend to contact HPG BigData developers by writing to OpenCB mailing list opencb@googlegroups.com. The main developers and maintainers are:
* Ignacio Medina (im411@cam.ac.uk) (_Founder and Project Leader_)
* Joaquín Tárraga (jtarraga@cipf.es)
* Jacobo Coll (jacobo.coll-moragon@genomicsengland.co.uk)

##### Other Contributors
* Matthias Haimel (mh719@cam.ac.uk)
* Jose M. Mut (jmmut@ebi.ac.uk)

##### Contributing
HPG BigData is an open-source and collaborative project. We appreciate any help and feeback from users, you can contribute in many different ways such as simple bug reporting and feature request. Dependending on your skills you are more than welcome to develop client tools, new features or even fixing bugs.


# How to build
HPG BigData is mainly developed in Java and it uses [Apache Maven](http://maven.apache.org/) as building tool. HPG BigData requires Java 8 and others OpenCB Java dependencies that can be found in [Maven Central Repository](http://search.maven.org/).

Stable releases are merged and tagged at **_master_** branch, you are encourage to use latest stable release for production. Current active development is carried out at **_develop_** branch, only compilation is guaranteed and bugs are expected, use this branch for development or for testing new functionalities. Only dependencies of **_master_** branch are ensured to be deployed at [Maven Central Repository](http://search.maven.org/), **_develop_** branch may require users to download and install other active OpenCB repositories:

* _GA4GH_: https://github.com/opencb/ga4gh (branch 'master')
* _java-common-libs_: https://github.com/opencb/java-common-libs (branch 'develop')
* _biodata_: https://github.com/opencb/biodata (branch 'develop')

To build the application, run `./build.sh` on the main folder. It will create a new folder **build**. Find the launch scripts on **build/bin**, and some examples on **build/examples**.

### System requirements
These are other requirements:

* Java 1.8
* cmake, g++
* Libraries: libz, libsnappy, liblzma, libncurses


### Cloning
HPG BigData is an open-source and free project, you can download **_develop_** branch by executing:

    imedina@ivory:~$ git clone https://github.com/opencb/hpg-bigdata.git
    Cloning into 'hpg-bigdata'...
    remote: Counting objects: 3206, done.
    remote: Compressing objects: 100% (118/118), done.
    remote: Total 3206 (delta 47), reused 0 (delta 0), pack-reused 3039
    Receiving objects: 100% (3206/3206), 11.54 MiB | 1.35 MiB/s, done.
    Resolving deltas: 100% (913/913), done.
    Checking connectivity... done.


### Running
For command line options invoke:

    imedina@ivory:~$ cd build/bin
    imedina@ivory:~$ ./hpg-bigdata.sh -h
    imedina@ivory:~$ ./hpg-bigdata-local.sh -h


##### _convert_ command
The command **convert** al lows you to save Fastq, SAM, BAM, VCF,... files as Avro files according to the GA4GH models. You can specify a compression method, e.g., deflate, snappy, bzip2.
The source files (Fastq, SAM, BAM...) have to be located in the local file system, on the other hand, destination files can be saved both in the local file system and in the Hadoop file system (HDFS), in the latter case, you must use the notation **hdfs://**
  
  Some examples using the test files in the folder data:
   
    $ ./hpg-bigdata-local.sh convert -c fastq2avro -i ../data/test.fq -o ../data/test.fq.avro -x snappy
    $ ./hpg-bigdata.sh convert -c fastq2avro -i hdfs://../data/test.fq -o hdfs://test.fq.hdfs.avro -x snappy
    
    $ ./hpg-bigdata-local.sh convert -c sam2avro -i ../data/test.sam -o ../data/test.sam.avro -x bzip2
    $ ./hpg-bigdata.sh convert -c sam2avro -i hdfs://../data/test.sam -o hdfs://test.sam.hdfs.avro -x bzip2
    
    $ ./hpg-bigdata-local.sh convert -c bam2avro -i ../data/test.bam -o ../data/test.bam.avro -x deflate
    $ ./hpg-bigdata.sh convert -c bam2avro -i hdfs://../data/test.bam -o hdfs://test.bam.hdfs.avro -x deflate

    $ ./hpg-bigdata-local.sh convert -c vcf2avro -i ../data/test.vcf -o test.vcf.avro -x snappy
    $ ./hpg-bigdata.sh convert -c vcf2avro -i hdfs://../data/test.vcf -o hdfs://test.vcf.gz.hdfs.avro -x snappy

  In addition, by using the command **convert**, you can save the Avro files as the original formats (Fastq, SAM, BAM...). In this case, the Avro files can be located both in the local file system and in the HDFS. 
  
  Some examples:
   
    $ ./hpg-bigdata-local.sh convert -c avro2fastq -i ../data/test.fq.avro -o ../data/test.fq.avro.fq
    $ ./hpg-bigdata.sh convert -c avro2fastq -i hdfs://../datat/est.fq.hdfs.avro -o ../data/test.fq.hdfs.avro.fq
    
    $ ./hpg-bigdata-local.sh convert -c avro2sam -i ../data/test.sam.avro -o ../data/test.sam.avro.sam
    $ ./hpg-bigdata.sh convert -c avro2sam -i hdfs://../data/test.sam.hdfs.avro -o ../data/test.sam.hdfs.avro.sam
    
    $ ./hpg-bigdata-local.sh convert -c avro2bam -i ../data/test.bam.avro -o ../data/test.sam.avro.bam
    $ ./hpg-bigdata.sh convert -c avro2bam -i hdfs://../data/test.bam.hdfs.avro -o ../data/test.bam.hdfs.avro.bam
   

##### _fastq_ command
  The command **fastq** allows you to compute some statistical values for a given Fastq file that must be stored in the Haddop/HDFS environment, and according to the GA4GH Avro models (Check the command **convert**)
  
  Some examples:
   
    $ ./hpg-bigdata.sh fastq --stats -i hdfs://test.fq.ga -o /tmp/stats-test.fq
    $ ./hpg-bigdata.sh fastq --kmers 7 -i hdfs://test.fq.ga -o /tmp/kmers-test.fq
    $ ./hpg-bigdata.sh fastq --stats --kmers 7 -i hdfs://test.fq.ga -o /tmp/stats-kmers-test.fq

##### _bam_ command
  The command **bam** allows you to compute some statistical values and the coverage or depth for a given BAM file that must be stored in the Haddop/HDFS environment, and according to the GA4GH Avro models (Check the command **convert**).

  Some examples:

    $ ./hpg-bigdata.sh bam --command stats -i hdfs:///test.bam.avro/part-r-00000.avro -o /tmp/bam-stats
    $ ./hpg-bigdata.sh bam --command depth -i hdfs:///test.bam.avro/part-r-00000.avro -o /tmp/bam-stats


# Supporters
JetBrains is supporting this open source project with:

[![Intellij IDEA](https://www.jetbrains.com/idea/docs/logo_intellij_idea.png)]
(http://www.jetbrains.com/idea/)
