# ESGF Virtual Aggregation Project

The aim of this project is to create a ready-to-deploy TDS catalog including **ALL** available data in ESGF, using OpenDAP endpoints to provide ESGF data analysis while avoiding the download of any data from remote repositories.

You can access the alpha version of the project at https://data.meteo.unican.es/thredds/catalog/devel/EVA/catalog.html.

**Why?** Because we think that currently ESGF does not make use of state of the art capabilities in their current software stack that would easily provide additional useful services for end users.

The idea is as follows:

1 - Use `esgf-search` to query available information.

2 - Store file metadata locally using `PyTables` to efficiently perform queries.

3 - Create THREDDS NcMLs and catalogs via `SantanderMetGroup/publisher` and OpenDAP endpoints.

4 - Provide multiple virtual views of the datasets:
  - ESGF dataset - Aggregate files in the same way that `esgf-search type=Dataset`.
  - Variable aggregation - Aggregate all variables into a single dataset.
  - Ensemble aggregation - Aggregate all variables and ensembles into a single dataset.

## ESGF projects

ESGF Virtual Aggregation datasets are separately constructed for each ESGF project (CMIP6, CORDEX, CMIP5, ...) due to changes in metedata definition accros projects.

Columns for CMIP6:

```
"HTTPServer","OPENDAP","_timestamp","_version_","activity_id","checksum","checksum_type","citation_url","data_node","data_specs_version","dataset_id","experiment_id","experiment_title","frequency","further_info_url","grid","grid_label","id","index_node","instance_id","latest","master_id","member_id","mip_era","model_cohort","nominal_resolution","pid","product","project","realm","replica","retracted","score","size","source_id","source_type","sub_experiment_id","table_id","timestamp","title","tracking_id","type","url","variable","variable_id","variable_long_name","variable_units","variant_label","version"
```

Columns for CORDEX:

```
"HTTPServer","OPENDAP","_timestamp","_version_","cf_standard_name","checksum","checksum_type","data_node","dataset_id","domain","driving_model","ensemble","experiment","experiment_family","id","index_node","instance_id","institute","latest","master_id","product","project","rcm_name","rcm_version","replica","retracted","score","size","time_frequency","timestamp","title","tracking_id","type","url","variable","variable_long_name","variable_units","version"
```

## Usage

`./search.sh` - Comment lines for projects you want to ignore. Take into account that for CMIP6 this takes a couple of days.
`python dump.py [--overwrite]` - Generate CSV files for datasets.
`./ncmls.sh` - Generate NcMLs from input files generated from `dump.py`.

I use a job script like the following:

```bash
#!/bin/bash

python dump.py --dest $DATASETS $HDF | ./publisher.sh
```

## Regular update

## Contact
