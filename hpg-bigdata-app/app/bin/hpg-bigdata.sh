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

native=${DIR}/../native

export LD_LIBRARY_PATH=${DIR}/../libs/

echo "Executing: $HADOOPCMD jar ${DIR}/../libs/*.jar $@"
$HADOOPCMD jar ${DIR}/../libs/*.jar $@