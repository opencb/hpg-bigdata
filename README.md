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


As you can see there are three commands implemnted, each of them with different subcommands.

 * **_sequence_** : to process FastQ sequence files
 * **_alignment_**: to process BAM alignment files
 * **_variant_**  : to process VCF variant files

You can find more detailed documentation and tutorials at: https://github.com/opencb/hpg-bigdata/wiki.

# Supporters
JetBrains is supporting this open source project with:

[![Intellij IDEA](https://www.jetbrains.com/idea/docs/logo_intellij_idea.png)]
(http://www.jetbrains.com/idea/)
