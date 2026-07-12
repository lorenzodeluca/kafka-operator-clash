#!/bin/bash

# Claim conditions timestamps
kubectl get datastreams.messaging.lorenzodeluca.it lorenzo-sandbox-stream -n default -o json \
| jq -r '.status.conditions[] | [.type,.status,.reason,.lastTransitionTime] | @tsv'

# XR conditions timestamps (get current XR name first)
XR=$(kubectl get xdatastreams.messaging.lorenzodeluca.it \
  -l crossplane.io/claim-name=lorenzo-sandbox-stream,crossplane.io/claim-namespace=default \
  -o jsonpath='{.items[0].metadata.name}')
echo "XR=$XR"

kubectl get xdatastreams.messaging.lorenzodeluca.it "$XR" -o json \
| jq -r '.status.conditions[] | [.type,.status,.reason,.lastTransitionTime,.message] | @tsv'

# Topic conditions timestamps
kubectl get topics.topic.kafka.crossplane.io -o json \
| jq -r '.items[] | .metadata.name as $n | .status.conditions[]? | [$n,.type,.status,.reason,.lastTransitionTime] | @tsv'
