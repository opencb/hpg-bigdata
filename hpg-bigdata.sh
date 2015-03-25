#!/bin/bash

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

if [ $hadoop -eq 1 ];
then
	echo "Executing in a Hadoop environment"
	hadoop jar hpg-bigdata-app/target/hpg-bigdata-app-0.1.0-jar-with-dependencies.jar $@
else
	echo "Executing in a local environment"
	java -jar hpg-bigdata-app/target/hpg-bigdata-app-0.1.0-jar-with-dependencies.jar $@
fi