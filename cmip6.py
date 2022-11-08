import os,sys,errno
import cftime
import pandas as pd

source = os.path.abspath(sys.argv[1])
df = pd.read_pickle(source)

# llnl requires https in OPENDAP URLs
subset = df[('GLOBALS', 'data_node')] == 'aims3.llnl.gov'
df.loc[subset, ('GLOBALS', 'OPENDAP')] = df.loc[subset, ('GLOBALS', 'OPENDAP')].str.replace('^http://', 'https://', regex=True)

# some versions contain "v" at the beggining, others do not
df[('GLOBALS', 'version')] = df[('GLOBALS', 'version')].str.lstrip('v')

# fix time values per variable
df[('GLOBALS', '_modified_time_coord')] = False
df[('GLOBALS', '_CoordinateAxes')] = None
for varname, vargroup in df[df[('GLOBALS', 'frequency')] != "fx"].groupby( ('GLOBALS', 'variable_id') ):
    # always create a time coordinate per variable
    df.loc[vargroup.index, (varname, '_dimensions')] = \
        df.loc[vargroup.index, (varname, '_dimensions')].str.replace(r'\btime\b', '_'.join(['time', varname]), regex=True)

    if ('_d_time', 'name') in df.columns:
        df.loc[vargroup.index, ('_d_time', 'name')] = '_'.join(['time', varname])

    if (varname, 'coordinates') in df.columns:
        df.loc[vargroup.index, (varname, 'coordinates')] = \
            df.loc[vargroup.index, (varname, 'coordinates')].str.replace(r'\btime\b', '_'.join(['time', varname]), regex=True)

    if (varname, 'cell_methods') in df.columns:
        df.loc[vargroup.index, (varname, 'cell_methods')] = \
            df.loc[vargroup.index, (varname, 'cell_methods')].str.replace(r'\btime\b', '_'.join(['time', varname]), regex=True)

    # explicitly declare _CoordinateAxes for netcdf-java
    coords = [(varname, x) for x in ['coordinates', '_dimensions'] if (varname, x) in df.columns]
    df.loc[vargroup.index, ('GLOBALS', '_CoordinateAxes')] = df.loc[vargroup.index, coords].fillna('').agg(
        lambda x: ','.join([y for y in x if y != '']), axis=1)

    # modify time values if units or calendar change along the time series
    if (len(vargroup[('time', 'units')].unique()) > 1 or
        len(vargroup[('time', 'calendar')].unique()) > 1):

        i = ('GLOBALS', 'localpath')
        reference = vargroup.sort_values(i).iloc[0]
        reference_units = reference[('time', 'units')]
        reference_calendar = reference[('time', 'calendar')]
    
        subset = [('time', 'units'), ('time', 'calendar'), ('time', '_values')]
        cftimes = vargroup[subset].apply(lambda row:
            cftime.num2date(row[('time', '_values')], row[('time', 'units')], row[('time', 'calendar')]), axis=1)
        df.loc[vargroup.index, ('time', '_values')] = cftimes.apply(
            lambda dates: cftime.date2num(dates, reference_units, reference_calendar))

        df.loc[vargroup.index, ('time', 'calendar')] = reference_calendar
        df.loc[vargroup.index, ('time', 'units')] = reference_units
        df.loc[vargroup.index, ('GLOBALS', '_modified_time_coord')] = True

df.to_pickle(source)

try:
    print(source)
except IOError as e:
    if e.errno == errno.EPIPE:
        print("cmip6.py on {} couldn't write to PIPE".format(source), file=sys.stderr)
        sys.exit(1)
