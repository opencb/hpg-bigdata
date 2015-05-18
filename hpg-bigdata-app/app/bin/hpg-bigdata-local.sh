#!/bin/bash

## find script directory
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

## Parallel threads for vcf2ga conversion using linux
parallel="-Dga4gh.vcf2ga.parallel=4"

# If a specific java binary isn't specified search for the standard 'java' binary
if [ -z "$JAVACMD" ] ; then
  if [ -n "$JAVA_HOME"  ] ; then
    if [ -x "$JAVA_HOME/jre/sh/java" ] ; then
      # IBM's JDK on AIX uses strange locations for the executables
      JAVACMD="$JAVA_HOME/jre/sh/java"
    else
      JAVACMD="$JAVA_HOME/bin/java"
    fi
  else
    JAVACMD=`which java`
  fi
fi

native=${DIR}/../native

export LD_LIBRARY_PATH=${DIR}/../libs/

$JAVACMD $parallel -classpath ${DIR}/../libs/*.jar org.opencb.hpg.bigdata.app.BigDataLocalMain $@
