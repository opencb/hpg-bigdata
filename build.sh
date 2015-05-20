#!/bin/bash
set -e
mkdir -p build
cp -r hpg-bigdata-app/app/* build/

mvn -T 2 clean install -DskipTests

cd hpg-bigdata-core/native
./build.sh
cd ../..

PLATFORM=`uname -s`

## Copy other files into build folder.
mkdir -p build/libs

if [[ "Darwin" == "$PLATFORM" ]]; then
	cp hpg-bigdata-core/native/third-party/htslib/libhts.*dylib build/libs/
	cp hpg-bigdata-core/native/third-party/avro-c-1.7.7/build/src/libavro.*dylib build/libs/
	cp hpg-bigdata-core/native/libhpgbigdata.dylib build/libs/
else
	cp hpg-bigdata-core/native/third-party/htslib/libhts.so* build/libs/
	cp hpg-bigdata-core/native/third-party/avro-c-1.7.7/build/src/libavro.so* build/libs/
    cp hpg-bigdata-core/native/libhpgbigdata.so build/libs/
fi

cp README.md build/
cp LICENSE build/

chmod +x build/bin/*.sh
chmod +x build/examples/*.sh