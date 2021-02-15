import sys, os
import json
import tables

table = tables.open_file('cmip6.hdf5', mode='r')
files = table.get_node('/files')

datasets = [] 
for row in files:
    if row['_eva_variable_aggregation'] not in datasets:
        datasets.append(row['_eva_variable_aggregation'])

for dataset in datasets:
    current = files.read_where('''(_eva_variable_aggregation == {}) & (data_node != b"esgf-data2.diasjp.net") & (data_node != b"esgf-data3.diasjp.net")'''.format(dataset), field='url')
    path = os.path.abspath("tests/" + dataset.decode('utf-8'))
    fh = open(path, 'w')
    for url in current:
        print(url.decode('utf-8'), file=fh)
    fh.close()

table.close()
