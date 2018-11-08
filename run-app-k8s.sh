#!/usr/bin/env bash

kubectl delete pod spark-driver

/Users/ruslan_mendybayev/dist/spark-2.4.0-bin-db-extractor-spark/bin/spark-submit \
    --class org.dbtofile.TableImport \
    --master k8s://https://192.168.64.16:8443 \
    --deploy-mode cluster \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=rmendybayev/develop:dbtofile_0.8.1 \
    --conf spark.kubernetes.driver.pod.name=spark-driver \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.appconfpvc.mount.path=/opt/conf \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.appconfpvc.mount.readOnly=false \
    --conf spark.kubernetes.driver.volumes.persistentVolumeClaim.appconfpvc.options.claimName=spark-conf-k8s-pvc \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pvc.mount.path=/opt/exec \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pvc.mount.readOnly=false \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pvc.options.claimName=spark-exec-k8s-pvc \
    --driver-java-options -Dconfig.file=/opt/conf/app.conf \
    --driver-class-path /opt/app/dbtofile/target/dbtofile-1.0-SNAPSHOT.jar \
    local:///opt/app/dbtofile/target/dbtofile-1.0-SNAPSHOT.jar
