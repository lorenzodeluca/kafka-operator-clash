#!/bin/bash

bash scripts/collect-resource-usage.sh at-rest 60 5
bash scripts/collect-resource-usage.sh scale crossplane 1,10,50,100,500,1000 60 5
bash scripts/collect-resource-usage.sh scale kubebuilder 1,10,50,100,500,1000 60 5
