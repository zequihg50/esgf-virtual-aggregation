import sys
import tables

project = sys.argv[2]
#fields = sys.argv[3].split(',')

f = tables.open_file(sys.argv[1], mode='r')
files = f.get_node('/files')

last = ''
for dataset in files.colinstances[project]:
    if last != dataset:
        last = dataset
        current = files.read_where('''{} == {}'''.format(project, last), field='OPENDAP')
        dest = 'by-dataset/{}'.format(dataset.decode('utf-8'))
        fh = open(dest, 'w')
        for url in current:
            print(url.decode('utf-8'), file=fh)
        fh.close()

f.close()
