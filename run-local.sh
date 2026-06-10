#!/bin/bash

mvn compile exec:java \
  -Dexec.mainClass=org.example.DigitransitHfpPipeline \
  -Dexec.args="--runner=DirectRunner \
    --project=project-b2f8b5ee-abac-4b3f-940 \
    --pubsubTopic=hfp-vp \
    --logMessages=true"
