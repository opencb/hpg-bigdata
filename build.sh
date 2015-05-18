#!/bin/bash

mkdir --parents build

mvn -T 2 clean install -DskipTests

cd hpg-bigdata-core/native
./build.sh
cd ../..

## Copy other files into build folder.
cp -r hpg-bigdata-core/native/libhpgbigdata.so build/libs/
cp -r hpg-bigdata-core/native/third-party/htslib/libhts.so* build/libs/
cp -r hpg-bigdata-core/native/third-party/avro-c-1.7.7/build/src/libavro.so* build/libs/
cp -r data/ build/
cp README.md build/
cp LICENSE build/

chmod +x build/bin/*.sh
chmod +x build/examples/*.sh