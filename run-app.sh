#!/usr/bin/env bash

spark-submit \
    --class org.dbtofile.TableImport \
    --files conf/app.conf \
    --driver-java-options -Dconfig.file=conf/app.conf \
    --driver-class-path target/dbtofile-1.2-SNAPSHOT.jar \
    target/dbtofile-1.2-SNAPSHOT.jar