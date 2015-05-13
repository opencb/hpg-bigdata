#!/bin/bash

echo
echo "Building library avro-c-1.7.7"
cd third-party/avro-c-1.7.7/
if [ ! -d "build" ]; then
  mkdir build
fi
cd build
cmake .. -DCMAKE_BUILD_TYPE=RelWithDebInfo
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

cd ../..
echo 
echo "Building the dynamic library libhpgbigdata.so"
gcc -O3 -std=gnu99 ./converters/bam2ga.c jni/org_opencb_hpg_bigdata_core_NativeSupport.c -o libhpgbigdata.so -shared -fPIC -I third-party/avro-c-1.7.7/src/ -I $JAVA_HOME/include -I $JAVA_HOME/include/linux -I third-party/ -I third-party/htslib/ -L third-party/avro-c-1.7.7/build/src/ -L third-party/htslib/ -lhts -lavro -lpthread