#!/bin/bash

## find script directory
DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

## Parallel threads for vcf2ga conversion using linux
parallel="-Dconvert.vcf2avro.parallel=4"

# If a specific hadoop binary isn't specified search for the standard 'hadoop' binary
if [ -z "$HADOOPCMD" ] ; then
  if [ -n "$HADOOP_HOME"  ] ; then
    HADOOPCMD="$HADOOP_HOME/bin/hadoop"
  else
    HADOOPCMD=`which hadoop`
  fi
fi

version=${hpg.version}
native=${DIR}/../native

export LD_LIBRARY_PATH=${DIR}/../libs/

export HADOOP_CLASSPATH="${DIR}/../libs/avro/avro-1.7.7.jar:${DIR}/../libs/avro/avro-mapred-1.7.7-hadoop2.jar"
export HADOOP_USER_CLASSPATH_FIRST=true

#echo "Executing: $HADOOPCMD jar ${DIR}/../libs/*.jar -libjars ${LIBJARS} $@"
$HADOOPCMD jar ${DIR}/../libs/hpg-bigdata-app-${version}-jar-with-dependencies.jar $@