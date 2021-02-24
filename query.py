import sys
import tables

import config

def parse_args(argv):
    args = {
        'project': '_eva_ensemble_aggregation',
        'projects': config.projects,
        'table': None,
    }

    pos = 1
    arguments = len(argv) - 1

    while arguments >= pos:
        if argv[pos] == '--projects':
            args['projects'] = argv[pos+1].split(',')
            pos+=2
        else:
            args['table'] = argv[pos]
            pos+=1

    return args

args = parse_args(sys.argv)
if args['table'] is None:
    print('No input file provided, exiting...', file=sys.stderr)

#fields = sys.argv[3].split(',')

f = tables.open_file(args['table'], mode='r')
files = f.get_node('/files')
project = args['project']

for i in sys.stdin:
    i = i.rstrip('\n')
    i_bytes = bytes(i, 'utf-8')
    matches = files.where('''{} == {}'''.format(project, i_bytes))
    dest = 'by-ensemble/{}'.format(i)
    fh = open(dest, 'w')
    for match in matches:
        print(match['OPENDAP'].decode('utf-8'), file=fh)
    fh.close()
    print(dest)

f.close()
