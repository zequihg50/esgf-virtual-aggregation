import os, sys
import numpy as np
import tables

def parse_args(argv):
    args = {
        'from': None,
        'dest': None,
        'fields': ['OPENDAP','index_node','data_node','size','replica','version','retracted','_timestamp','_version_','checksum','checksum_type','_eva_ensemble_aggregation','_eva_variable_aggregation','_eva_no_frequency'],
        'frequency': 'frequency',
        'overwrite': False,
        'step': 100000,
    }

    arguments = len(argv) - 1
    position = 1
    while arguments >= position:
        if argv[position] == '-h' or argv[position] == '--help':
            print('RTFM', file=sys.stderr)
            sys.exit(1)
        elif argv[position] == '-d' or argv[position] == '--dest':
            args['dest'] = argv[position+1]
            position+=2
        elif argv[position] == '-f' or argv[position] == '--fields':
            args['fields'] = argv[position+1].split(',')
            position+=2
        elif argv[position] == '--frequency':
            args['frequency'] = argv[position+1]
            position+=2
        elif argv[position] == '--overwrite':
            args['overwrite'] = True
            position+=1
        elif argv[position] == '--step':
            args['step'] = argv[position+1]
            position+=2
        else:
            args['from'] = argv[position]
            position+=1

    return args

args = parse_args(sys.argv)

os.makedirs(args['dest'], exist_ok=True)

f = tables.open_file(args['from'], 'r')
t = f.get_node('/files')

step = args['step']
s = set()
for i in range(len(t)//step + 1):
    s.update( t.colindexes['_eva_ensemble_aggregation'].read_sorted(i*step, i*step+step) )

a = np.array(list(s))
for agg in a[a != b'']:
    dest = os.path.join(args['dest'], agg.decode('utf-8'))
    if os.path.exists(dest) and (not args['overwrite']) and os.stat(dest).st_size != 0:
        continue

    # find current aggregation files and corresponding fxs
    dataset = t.read_where('''(_eva_ensemble_aggregation == {}) & (frequency != b"fx")'''.format(agg))
    if len(dataset) == 0:
        continue

    fx_agg = dataset[0]['_eva_no_frequency']
    fxs = t.read_where('''(_eva_no_frequency == {}) & ({} == b"fx")'''.format(fx_agg, args['frequency']))

    dataset = np.concatenate([dataset, fxs], axis=0)
    fh = open(dest, 'w')
    for match in dataset:
        fields = ','.join( [match[field].decode('utf-8') for field in args['fields']] )
        if fields != '' and len(match['OPENDAP']) > 0:
            print(fields, file=fh)
    fh.close()
    print(dest)

f.close()
