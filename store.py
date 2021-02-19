import sys, os
import json
import tables

MAX_SIZE = 1000 # 1000 characters

projects = [
    "_eva_esgf_dataset",
    "_eva_variable_aggregation",
    "_eva_variable_aggregation_levels",
    "_eva_ensemble_aggregation",
    "_eva_ensemble_aggregation_levels",
]

columns = projects + ["HTTPServer", "OPENDAP", "_timestamp", "_version_", "activity_id", "checksum", "checksum_type", "citation_url", "data_node", "data_specs_version", "dataset_id", "experiment_id", "experiment_title", "frequency", "further_info_url", "grid", "grid_label", "id", "index_node", "instance_id", "latest", "master_id", "member_id", "mip_era", "model_cohort", "nominal_resolution", "pid", "product", "project", "realm", "replica", "retracted", "score", "size", "source_id", "source_type", "sub_experiment_id", "table_id", "timestamp", "title", "tracking_id", "type", "url", "variable", "variable_id", "variable_long_name", "variable_units", "variant_label", "version"]

schema = dict(zip(columns, [tables.StringCol(MAX_SIZE)]*len(columns)))

# Create file, table and arrays
f = tables.open_file(sys.argv[1], mode='w')
filt = tables.Filters(complevel=1, shuffle=True)
for eva_aggregation in projects:
    f.create_earray(
        f.root,
        eva_aggregation,
        tables.StringAtom(MAX_SIZE),
        (0,),
        eva_aggregation,
        filters=filt)
files = f.create_table(f.root, 'files', schema, 'files', filters=filt, expectedrows=50000000)

# Populate table and arrays
row = files.row
for line in sys.stdin:
    d = json.loads(line.rstrip('\n'))
    for c in columns:
        if c in d:
            row[c] = d[c]
    row.append()

for eva_aggregation in projects:
    arr = f.get_node('/' + eva_aggregation)
    for row in files:
        if row[eva_aggregation] not in arr:
            arr.append([ row[eva_aggregation] ])

# Index table
for eva_aggregation in projects:
    files.colinstances[eva_aggregation].create_index()

f.flush()
