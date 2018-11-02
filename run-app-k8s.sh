#!/usr/bin/env bash

spark-submit \
    --class org.dbtofile.TableImport \
    --master k8s://https://192.168.64.16:8443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=rmendybayev/develop:dbtofile_0.2 \
    --conf spark.kubernetes.driver.pod.name=spark-driver \
    --driver-java-options -Dconfig.file=/opt/app/dbtofile/conf/app.conf \
    --driver-class-path /opt/app/dbtofile/target/dbtofile-1.0-SNAPSHOT.jar \
    local:///opt/app/dbtofile/target/dbtofile-1.0-SNAPSHOT.jar