#!/bin/bash

mkdir --parents build
cp -r hpg-bigdata-app/app/* build/

mvn -T 2 clean install -DskipTests

cd hpg-bigdata-core/native
./build.sh
cd ../..

## Copy other files into build folder.
mkdir --parents build/libs
cp hpg-bigdata-core/native/libhpgbigdata.so build/libs/
cp hpg-bigdata-core/native/third-party/htslib/libhts.so* build/libs/
cp hpg-bigdata-core/native/third-party/avro-c-1.7.7/build/src/libavro.so* build/libs/
cp README.md build/
cp LICENSE build/

chmod +x build/bin/*.sh
chmod +x build/examples/*.sh