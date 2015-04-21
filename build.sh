#!/bin/bash

mvn install -DskipTests

cd hpg-bigdata-core/native
./build.sh
cd ../..
