#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )


${DIR}/../bin/hpg-bigdata-local.sh convert -c bam2ga -i ${DIR}/../data/test.bam -o ${DIR}/../data/test.bam.avro
