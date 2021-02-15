#!/bin/bash

utils=esgf-utils
facets_url='https://esgf-data.dkrz.de/esg-search/search?facets=source_id&project=CMIP6&limit=0&format=application%2Fsolr%2Bjson' 

curl -s "${facets_url}" | jq -r '.facet_counts.facet_fields.source_id|map(strings)[]' | while read source_id
do
  ${utils}/esgf-search -q "project=CMIP6 type=File source_id=${source_id}" -m 100 | jq -c '
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
done | python store.py cmip6.hdf5
