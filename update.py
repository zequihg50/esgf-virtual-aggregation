import sys
import json
import tables

from config import projects

f = tables.open_file(sys.argv[1], 'r+')
files = f.get_node('/files')
columns = files.colnames
row = files.row

field = 'id'
project_to_filter = '_eva_variable_aggregation'

for line in sys.stdin:
    d = json.loads(line.rstrip('\n'))

    # check if id already exists
    exists = False
    query = '''{} == {}'''.format(project_to_filter, bytes(d[project_to_filter], 'utf-8'))
    for stored in files.where(query):
        byts = bytes(d[field], 'utf-8')
        if stored[field] == byts:
            exists = True

    # does not exist, store
    if not exists:
        for key in d:
            if key in columns:
                row[key] = d[key]
        row.append()

        # notify changes, note that this will produce duplicates, use sort -u
        print(d[project_to_filter])

files.flush()
