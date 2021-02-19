import sys
import tables

project = sys.argv[2]
#fields = sys.argv[3].split(',')

f = tables.open_file(sys.argv[1], mode='r')
files = f.get_node('/files')

project = '_' + project.lstrip('_')
for d in f.get_node('/' + project):
    current = files.read_where('''{} == {}'''.format(project, d), field='OPENDAP')
    dest = 'by-dataset/{}'.format(d.decode('utf-8'))
    fh = open(dest, 'w')
    for url in current:
        print(url.decode('utf-8'), file=fh)
    fh.close()

f.close()
