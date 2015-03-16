#!/bin/bash

if [ $# -eq 1 ]; then
    if [ $1 == 'run-maven' ]; then
        mvn clean install -DskipTests
    fi
fi

## Check if 'build' folder exists to delete the content. If not it is created.
if [ -d build ]; then
    rm -rf build/*
else
    mkdir build
fi

## Copy all the binaries, dependencies and files
cp -r hpg-bigdata-app/target/appassembler/* build/
cp README.md build/

