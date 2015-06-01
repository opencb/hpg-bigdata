#!/bin/bash

PLATFORM=`uname -s`

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

olib="libhpgbigdata.so"
if [[ "Darwin" == "$PLATFORM" ]]; then
  olib="libhpgbigdata.dylib"
fi

echo 
echo "Building the dynamic library $olib"

gcc -O3 -std=gnu99 ./converters/bam2ga.c jni/org_opencb_hpg_bigdata_core_NativeConverter.c -o $olib -shared -fPIC -I third-party/avro-c-1.7.7/src/ -I jni/ -I $JAVA_HOME/include -I $JAVA_HOME/include/linux -I $JAVA_HOME/include/darwin -I third-party/ -I third-party/htslib/ -L third-party/avro-c-1.7.7/build/src/ -L third-party/htslib/ -lhts -lavro -lpthread

olib="libhpgaligner.so"
if [[ "Darwin" == "$PLATFORM" ]]; then
  olib="libhpgaligner.dylib"
fi

echo
echo "Building the dynamic library $olib"

#gcc -O3 -std=gnu99 jni/org_opencb_hpg_bigdata_core_NativeAligner.c ./third-party/hpg-aligner/index.c ./third-party/hpg-aligner/mapper.c -o $olib -shared -fPIC -I jni/ -I third-party/hpg-aligner -I $JAVA_HOME/include -I $JAVA_HOME/include/linux -I $JAVA_HOME/include/darwin


#gcc -O3 -std=gnu99 src/*.c src/rna/*.c src/dna/*.c src/build-index/*.c src/tools/bam/aux/*.c src/tools/bam/recalibrate/*.c src/tools/bam/aligner/*.c -o final.so -shared -fPIC -I src -I src/rna -I src/dna -I src/build-index/ -I src/tools/bam/ -I src/tools/bam/recalibrate/ -I src/tools/bam/aligner/ -I lib/hpg-libs/third_party/ -I lib/hpg-libs/third_party/htslib/ -I lib/hpg-libs/c/src/ -I jni/ -I third-party/hpg-aligner -I $JAVA_HOME/include -I $JAVA_HOME/include/linux -I $JAVA_HOME/include/darwin

hpg_alinger_home="../../../hpg-aligner"
hpg_libs_home="../../../hpg-aligner/lib/hpg-libs/c/src"
cprops_home="../../../hpg-aligner/lib/hpg-libs/third_party"
gcc -O3 -std=gnu99 jni/org_opencb_hpg_bigdata_core_NativeAligner.c ./third-party/hpg-aligner/index.c \
  $cprops_home/cprops/avl.c                                  \
  $cprops_home/cprops/collection.c                           \
  $cprops_home/cprops/hashlist.c                             \
  $cprops_home/cprops/hashtable.c                            \
  $cprops_home/cprops/heap.c                                 \
  $cprops_home/cprops/linked_list.c                          \
  $cprops_home/cprops/log.c                                  \
  $cprops_home/cprops/mempool.c                              \
  $cprops_home/cprops/rb.c                                   \
  $cprops_home/cprops/util.c                                 \
  $cprops_home/cprops/vector.c                               \
  $cprops_home/cprops/trie.c                                 \
  $cprops_home/cprops/mtab.c                                 \
  $hpg_libs_home/commons/*.c                                 \
  $hpg_libs_home/containers/*.c                              \
  $hpg_libs_home/bioformats/fastq/*.c                        \
  $hpg_libs_home/bioformats/bam/*.c                          \
  $hpg_libs_home/aligners/bwt/bwt_commons.c                  \
  $hpg_libs_home/aligners/bwt/genome.c                       \
  $hpg_libs_home/aligners/bwt/bwt.c                          \
  $hpg_libs_home/aligners/sw/macros.c                        \
  $hpg_libs_home/aligners/sw/sse.c                           \
  $hpg_libs_home/aligners/sw/smith_waterman.c                \
  $hpg_alinger_home/src/*.c                                  \
  $hpg_alinger_home/src/rna/*.c                              \
  $hpg_alinger_home/src/dna/*.c                              \
  $hpg_alinger_home/src/dna/clasp_v1_1/*.c                   \
  $hpg_alinger_home/src/sa/*.c                               \
  $hpg_alinger_home/src/build-index/*.c                      \
  $hpg_alinger_home/src/tools/bam/aux/*.c                    \
  $hpg_alinger_home/src/tools/bam/recalibrate/*.c            \
  $hpg_alinger_home/src/tools/bam/aligner/*.c                \
  -o $olib -shared -fPIC                                     \
  -I $hpg_alinger_home/src                                   \
  -I $hpg_alinger_home/src/rna                               \
  -I $hpg_alinger_home/src/dna                               \
  -I $hpg_alinger_home/src/build-index/                      \
  -I $hpg_alinger_home/src/tools/bam/                        \
  -I $hpg_alinger_home/src/tools/bam/recalibrate/            \
  -I $hpg_alinger_home/src/tools/bam/aligner/                \
  -I $hpg_alinger_home/lib/hpg-libs/third_party/             \
  -I $hpg_alinger_home/lib/hpg-libs/third_party/htslib/      \
  -I $hpg_alinger_home/lib/hpg-libs/c/src/                   \
  -I jni/                                                    \
  -I third-party/hpg-aligner                                 \
  -I /usr/include/libxml2                                    \
  -I $JAVA_HOME/include                                      \
  -I $JAVA_HOME/include/linux                                \
  -I $JAVA_HOME/include/darwin
