#!/bin/bash

mvn compile exec:java \
  -Dexec.mainClass=org.example.DigitransitHfpPipeline \
  -Dexec.args="--runner=DataflowRunner \
    --project=project-b2f8b5ee-abac-4b3f-940 \
    --region=europe-west1 \
    --tempLocation=gs://hfp-dataflow-temp/temp \
    --usePublicIps=true \
    --defaultWorkerLogLevel=INFO \
    --numWorkers=1 \
    --workerMachineType=e2-standard-2 \
    --pubsubTopic=hfp-vp"
