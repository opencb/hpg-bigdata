#!/bin/bash

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cmd=$DIR/../bin/hpg-bigdata.sh
cmd_local=$DIR/../bin/hpg-bigdata-local.sh
data=${DIR}/../data

${cmd_local} convert -c vcf2ga -i ${data}/test.vcf -o ${data}/test.vcf.avro.avro
${cmd_local} convert -c vcf2ga -i ${data}/test.vcf -o ${data}/test.vcf.avro.avro.snz -x snappy
${cmd_local} convert -c vcf2ga -i ${data}/test.vcf -o ${data}/test.vcf.avro.avro.gz -x deflate
${cmd_local} convert -c vcf2ga -i ${data}/test.vcf -o ${data}/test.vcf.avro.avro.bz2 -x bzip2