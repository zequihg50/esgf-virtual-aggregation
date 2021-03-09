#!/bin/bash

set -e

trap exit SIGINT SIGKILL

jq -c '
{_timestamp,_version_,activity_id,checksum,checksum_type,citation_url,data_node,data_specs_version,dataset_id,experiment_id,experiment_title,frequency,further_info_url,grid,grid_label,id,index_node,instance_id,latest,master_id,member_id,mip_era,model_cohort,nominal_resolution,pid,product,project,realm,replica,retracted,score,size,source_id,source_type,sub_experiment_id,table_id,timestamp,title,tracking_id,type,url,variable,variable_id,variable_long_name,variable_units,variant_label,version} |
.url |= map(capture("(?<url>.*)\\|.*\\|(?<type>.*)")|{(.type): .url}) |
reduce .url[] as $item (.; . + $item) |
del(.url) |
if .OPENDAP then .OPENDAP |= sub("\\.html";"") else . end |
. +=(to_entries|map(select(.value|arrays))|map(.value |= first)|from_entries) |
. + {
  "_eva_esgf_dataset": [(.instance_id|split(".")[:10]|join(".")), .data_node]|join("@"),
  "_eva_variable_aggregation": [(.instance_id|split(".")[:10]|del(.[7])|join(".")), .data_node]|join("@"),
  "_eva_ensemble_aggregation": [(.instance_id|split(".")[:10]|del(.[5,7])|join(".")), .data_node]|join("@"),
  "_eva_no_frequency": [(.instance_id|split(".")[:10]|del(.[5,6,7])|join(".")), .data_node]|join("@")
}'
