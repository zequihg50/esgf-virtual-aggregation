#!/bin/bash

set -e

trap exit SIGINT SIGKILL

# CMIP6
subfield=variable_id
search_facets="esg-search/search?facets=${subfield}&project=CMIP6&limit=0&format=application%2Fsolr%2Bjson"
fields='_timestamp,_version_,activity_id,checksum,checksum_type,citation_url,data_node,data_specs_version,dataset_id,experiment_id,experiment_title,frequency,further_info_url,grid,grid_label,id,index_node,instance_id,latest,master_id,member_id,mip_era,model_cohort,nominal_resolution,pid,product,project,realm,replica,retracted,score,size,source_id,source_type,sub_experiment_id,table_id,timestamp,title,tracking_id,type,url,variable,variable_id,variable_long_name,variable_units,variant_label,version'

curl -s "http://esgf-data.dkrz.de/${search_facets}" | \
jq -r ".facet_counts.facet_fields.${subfield}|map(strings)[]" | \
while read variable
do
  esgf-utils/esgf-search -q "project=CMIP6 type=File variable_id=${variable}" | jq/cmip6.sh
done | python store.py --fields "${fields}" cmip6.hdf

# CORDEX
while read i
do
    esgf-utils/esgf-search -i "${i}" -q "project=CORDEX type=File distrib=False" | jq/cordex.sh
done < esgf-utils/indexnodes | python store.py --fields "${fields}" cordex.hdf

# CMIP5
