#!/bin/bash

set -e

trap exit SIGINT SIGKILL

jq -c '
{_timestamp,_version_,cf_standard_name,checksum,checksum_type,data_node,dataset_id,domain,driving_model,ensemble,experiment,experiment_family,id,index_node,instance_id,institute,latest,master_id,product,project,rcm_name,rcm_version,replica,retracted,score,size,time_frequency,timestamp,title,tracking_id,type,url,variable,variable_long_name,variable_units,version} |
.url |= map(capture("(?<url>.*)\\|.*\\|(?<type>.*)")|{(.type): .url}) |
reduce .url[] as $item (.; . + $item) |
del(.url) |
if .OPENDAP then .OPENDAP |= sub("\\.html";"") else . end |
. +=(to_entries|map(select(.value|arrays))|map(.value |= first)|from_entries) |
. + {
  "_eva_esgf_dataset": [(.instance_id|split(".")[:12]|join(".")), .data_node]|join("@"),
  "_eva_variable_aggregation": [(.instance_id|split(".")[:12]|del(.[10])|join(".")), .data_node]|join("@"),
  "_eva_ensemble_aggregation": [(.instance_id|split(".")[:12]|del(.[6,10])|join(".")), .data_node]|join("@"),
}'
