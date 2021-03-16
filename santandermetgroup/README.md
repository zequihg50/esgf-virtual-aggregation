# ESGF Virtual Aggregation Project for SantanderMetGroup

Find published data in the data node:

```bash
../esgf-utils/esgf-search -q "project=CORDEX data_node=data.meteo.unican.es type=File" | \
  ../jq/cordex.sh | \
  python ../esgf-utils/store.py --indexes _eva_ensemble_aggregation,_eva_no_frequency -d cordex.hdf
```

Dump datasets (requires certificate authentication using netCDF4-python, use .dodsrc with ESGF certificate):

```bash
python ../dump.py --frequency time_frequency -d datasets --prune cordex.hdf

# configure todf.py
columns=OPENDAP,index_node,data_node,size,replica,version,retracted,_timestamp,_version_,checksum,checksum_type,_eva_ensemble_aggregation,_eva_variable_aggregation,_eva_no_frequency
find datasets -type f | while read csv
do
    python -W ignore ../publisher/todf.py -f ${csv} --drs '.*/([^_]*)_.*' --drs-prefix '' --facets 'variable_id' --numeric size -v time --col 0 --cols ${columns} "pickles/{data_node}/{_eva_ensemble_aggregation}"
done
```

Standardize dataframe objects:

```bash
find pickles -type f | while read pickle
do
    python ../df.py "${pickle}"
done
```

NcMLs:

```bash
find pickles -type f | while read pickle
do
    python ../publisher/jdataset.py -t ../templates/cordex.ncml.j2 -d "ncmls/{_eva_ensemble_aggregation}.ncml" -o variable_col=variable -o eva_version=0 -o creation=Hola ${pickle}
done
```
