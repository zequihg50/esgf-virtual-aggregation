import sys, os
import json
import tables

MAX_SIZE = 750

def append(d, row, allowed_keys):
    for key in d:
        if key in allowed_keys:
            row[key] = d[key]
    row.append()

table = tables.open_file(sys.argv[1], mode='w')
filt = tables.Filters(complevel=1, shuffle=True)

# Guess table schema based on first json result from esgf-search
first = json.loads( sys.stdin.readline().rstrip('\n') )

projects = [
    "_eva_esgf_dataset",
    "_eva_variable_aggregation",
    "_eva_variable_aggregation_levels",
    "_eva_ensemble_aggregation",
    "_eva_ensemble_aggregation_levels",
]

File = dict(zip( projects,[tables.StringCol(MAX_SIZE)]*len(projects) ))
schema = dict(zip(first.keys(), [tables.StringCol(MAX_SIZE)]*len(first)))

File.update(schema)

files = table.create_table(table.root, 'files', File, 'files', filters=filt, expectedrows=50000000)
row = files.row

append(first, row, File.keys())
for line in sys.stdin:
    d = json.loads(line.rstrip('\n'))
    append(d, row, File.keys())

# https://github.com/PyTables/PyTables/issues/879
# Calculated based on 50.000.000 rows
blocksizes=(100103960, 50051980, 139810, 4096)
files.cols._eva_esgf_dataset.create_index(_blocksizes=blocksizes)
files.cols._eva_variable_aggregation.create_index(_blocksizes=blocksizes)
files.cols._eva_variable_aggregation_levels.create_index(_blocksizes=blocksizes)
files.cols._eva_ensemble_aggregation.create_index(_blocksizes=blocksizes)
files.cols._eva_ensemble_aggregation_levels.create_index(_blocksizes=blocksizes)
table.flush()
