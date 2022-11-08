import os, sys
import logging
import numpy as np
import tables

def parse_args(argv):
    args = {
        'from': None,
        'dest': None,
        'fields': ['OPENDAP','index_node','data_node','size','replica','version','retracted','_timestamp','_version_','checksum','checksum_type','_eva_ensemble_aggregation','_eva_variable_aggregation','_eva_no_frequency'],
        'frequency': 'frequency',
        'limit': None,
        'logfile': None,
        'loglevel': logging.INFO,
        'node': '/files',
        'overwrite': False,
        'overwrite_empty': False,
        'prune': False,
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
        elif argv[position] == '-l' or argv[position] == '--limit':
            args['limit'] = int(argv[position+1])
            position+=2
        elif argv[position] == '--logfile':
            args['logfile'] = argv[position+1]
            position+=2
        elif argv[position] == '--loglevel':
            args['loglevel'] = int(argv[position+1])
            position+=2
        elif argv[position] == '--node':
            args['node'] = argv[position+1]
            position+=2
        elif argv[position] == '--overwrite':
            args['overwrite'] = True
            position+=1
        elif argv[position] == '--overwrite-empty':
            args['overwrite_empty'] = True
            position+=1
        elif argv[position] == '--prune':
            args['prune'] = True
            position+=1
        elif argv[position] == '--step':
            args['step'] = argv[position+1]
            position+=2
        else:
            args['from'] = argv[position]
            position+=1

    return args

args = parse_args(sys.argv)
logging.basicConfig(filename=args['logfile'], format='%(asctime)s - %(message)s', level=args['loglevel'])

os.makedirs(args['dest'], exist_ok=True)

f = tables.open_file(args['from'], 'r')
t = f.get_node(args['node'])

s = set()
logging.debug('Loading unique IDs using a step of %d', args['step'])
for i in range(len(t)//args['step'] + 1):
    s.update( t.colindexes['_eva_ensemble_aggregation'].read_sorted(i*args['step'], i*args['step']+args['step']) )
logging.debug('Loaded %d unique IDs', len(s))

total = 0
for agg in sorted(filter(lambda x: x != b'', s)):
    dest = os.path.join(args['dest'], agg.decode('utf-8'))
    logging.debug('Preparing "%s"', dest)
    if os.path.exists(dest) and os.stat(dest).st_size == 0 and not args['overwrite_empty'] and not args['overwrite']:
        logging.debug('Ignoring dataset "%s" with size 0', dest)
        continue

    if os.path.exists(dest) and not args['overwrite']:
        logging.debug('Ignoring existing dataset "%s"', dest)
        continue

    # find current aggregation files and corresponding fxs
    dataset = t.where('''(_eva_ensemble_aggregation == {}) & (frequency != b"fx")'''.format(agg))

    fh = open(dest, 'w')
    fxid = b''
    for match in dataset:
        fxid = match['_eva_no_frequency']
        fields = ','.join( [match[field].decode('utf-8') for field in args['fields']] )
        if fields != '' and len(match['OPENDAP']) > 0:
            print(fields, file=fh)
    if fxid != b'':
        fxs = t.where('''(_eva_no_frequency == {}) & ({} == b"fx")'''.format(fxid, args['frequency']))
        for match in fxs:
            fields = ','.join( [match[field].decode('utf-8') for field in args['fields']] )
            if fields != '' and len(match['OPENDAP']) > 0:
                print(fields, file=fh)
    fh.close()

    # Check if generated dataset is empty
    total += 1
    if os.stat(dest).st_size == 0 and args['prune']:
        logging.debug('Removing empty dataset "%s"', dest)
        os.remove(dest)
    else:
        logging.info('Dumped dataset "%s"', dest)
        print(dest)

    # Stop if reached user's limit
    if args['limit'] is not None:
        if total == args['limit']:
            break

logging.info('Dumped %d datasets', total)
f.close()
