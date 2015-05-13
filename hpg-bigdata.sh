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

# init hadoop to 0
hadoop=0
params=""

for i in "$@"
do
  if [[ $i =~ ^hdfs://* ]];
  then
  	hadoop=1
  	params+=" "${i:7}
  else
    params+=" "$i
  fi
done

hbc="$DIR/hpg-bigdata-core"
hba="$DIR/hpg-bigdata-app"

export LD_LIBRARY_PATH=$hbc/native/:$hbc/native/third-party/avro-c-1.7.7/build/src/:$hbc/native/third-party/htslib/ 

if [ $hadoop -eq 1 ];
then
	echo "Executing in a Hadoop environment"
	hadoop jar $hba/target/hpg-bigdata-app-0.1.0-jar-with-dependencies.jar $@
else
	echo "Executing in a local environment"
	$JAVACMD $parallel -jar $hba/target/hpg-bigdata-app-0.1.0-jar-with-dependencies.jar $@
fi