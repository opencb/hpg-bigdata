#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cmd=$DIR/../bin/hpg-bigdata.sh
cmd_local=$DIR/../bin/hpg-bigdata-local.sh
data=${DIR}/../data


${cmd_local} convert -c bam2ga -i ${DIR}/../data/test.bam -o ${DIR}/../data/test.bam.avro
