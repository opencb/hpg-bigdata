#!/bin/bash

echo
echo "Building library avro-c-1.7.7"
cd third-party/avro-c-1.7.7/build/
make

cd ../../..
echo
echo "Building library htslib"
cd third-party/htslib
make

cd ../..
echo
echo "Building library samtools"
cd third-party/samtools
make HTSDIR=../htslib