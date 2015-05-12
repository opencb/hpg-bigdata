#!/bin/bash

mvn -T 2 clean install -DskipTests

cd hpg-bigdata-core/native
./build.sh
cd ../..
