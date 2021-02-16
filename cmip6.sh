#!/bin/bash

set -e

trap exit SIGINT SIGKILL

subfield=variable_id
search_facets="esg-search/search?facets=${subfield}&project=CMIP6&limit=0&format=application%2Fsolr%2Bjson"

curl -s "http://esgf-data.dkrz.de/${search_facets}" | \
jq -r ".facet_counts.facet_fields.${subfield}|map(strings)[]" | \
while read variable
do
  esgf-utils/esgf-search -q "project=CMIP6 type=File variable_id=${variable}" | jq -c '
    .url |= (map(select(endswith("OPENDAP")))|first) |
    select(.url != null) |
    .url |= (sub("\\|.*";"")|sub("\\.html";"")) |
    . +=(to_entries|map(select(.value|arrays))|map(.value |= first)|from_entries) |
    . + {
      "_eva_esgf_dataset": [(.instance_id|split(".")[:10]|join(".")), .data_node]|join("@"),
      "_eva_variable_aggregation": [(.instance_id|split(".")[:10]|del(.[7])|join(".")), .data_node]|join("@"),
      "_eva_variable_aggregation_levels": [(.instance_id|split(".")[:10]|del(.[7])|join(".")), .data_node]|join("@"),
      "_eva_ensemble_aggregation": [(.instance_id|split(".")[:10]|del(.[5,7])|join(".")), .data_node]|join("@"),
      "_eva_ensemble_aggregation_levels": [(.instance_id|split(".")[:10]|del(.[5,7])|join(".")), .data_node]|join("@")
    }'
done | python store.py cmip6.hdf
