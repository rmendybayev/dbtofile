#!/usr/bin/env bash

CONFFILE=$1

/opt/spark/bin/spark-submit \
    --class org.dbtofile.TableImport \
    --master k8s://http://fc8bdip01.gfoundries.com:8080 \
    --deploy-mode client \
    --conf spark.kubernetes.authenticate.submission.oauthToken=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJrdWJlcm5ldGVzL3NlcnZpY2VhY2NvdW50Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9uYW1lc3BhY2UiOiJrdWJlLXN5c3RlbSIsImt1YmVybmV0ZXMuaW8vc2VydmljZWFjY291bnQvc2VjcmV0Lm5hbWUiOiJjbHVzdGVyZGFzaGFkbWluLXRva2VuLXY3c3Z2Iiwia3ViZXJuZXRlcy5pby9zZXJ2aWNlYWNjb3VudC9zZXJ2aWNlLWFjY291bnQubmFtZSI6ImNsdXN0ZXJkYXNoYWRtaW4iLCJrdWJlcm5ldGVzLmlvL3NlcnZpY2VhY2NvdW50L3NlcnZpY2UtYWNjb3VudC51aWQiOiIzZDJlYWRmZS05NjhjLTExZTgtODRlOS0wYWVmY2IwMDAwMmUiLCJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQ6a3ViZS1zeXN0ZW06Y2x1c3RlcmRhc2hhZG1pbiJ9.gVzmZFUTQyeSRsh3yVj2gpzy13G4JD9XZeLMHC6dJThW753c2wBbpBZvhWWErYJPj2m4u9crlPUBWRlNKCM9ZoK-njAzjTBZJTfyQMj126160fUOnk4skHcv7xSxoYot-qNTrtOCHfCNLw49F4HevVNfJBpCelZn_fXqKB1luMfkoZvY3qSuYW196McN69wuNHfAFe58CNBscmEWRvIvkvUrXq_uaqJzvT5gLXujU9kFJ35z8uR5wq6h4dha6vptCyyeynpwGfm-EViRdv8hfAm_jL1BRyrNrrQ-rqKT1Z66yZUPirCUpkm1WQlIUxHMM-g5Fot6232_4qETy-uUNw \
    --conf spark.kubernetes.container.image=fc8kfkp01.gfoundries.com:5000/dbtofile:1.1 \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pvc.mount.path=/data \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pvc.mount.readOnly=false \
    --conf spark.kubernetes.executor.volumes.persistentVolumeClaim.pvc.options.claimName=spark-exec-k8s-pvc \
    --conf spark.driver.host=spark-driver \
    --conf spark.driver.port=6666 \
    --conf spark.executor.instances=4 \
    --conf spark.executor.memory=32g \
    --conf spark.executor.cores=8 \
    --driver-java-options -Dconfig.file=/opt/conf/$CONFFILE \
    --driver-class-path /opt/app/dbtofile/target/dbtofile-1.2.2-SNAPSHOT.jar \
    /opt/app/dbtofile/target/dbtofile-1.2.2-SNAPSHOT.jar