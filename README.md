# ESGF Virtual Aggregation Project

The aim of this project is to create a TDS catalog including **ALL** available data in ESGF, using OpenDAP endpoints to avoid downloading any data from remote repositories.

The idea is as follows:

1 - Use `esgf-search` to query available information.

2 - Store file metadata locally using `PyTables` to efficiently perform queries.

3 - Create THREDDS NcMLs and catalogs via `SantanderMetGroup/publisher` and OpenDAP endpoints.

4 - Provide multiple virtual views of the datasets:
  - Raw - Show files with no aggregation, just a virtual mirror of ESGF files like `esgf-search type=File`.
  - ESGF dataset - Aggregate files in the same way that `esgf-search type=Dataset`.
  - Variable aggregation - Aggregate all variables into a single dataset.
  - Variable levels aggregation - Aggregate all variables into a single dataset, one variable for multiple vertical levels.
  - Ensemble aggregation - Aggregate all variables and ensembles into a single dataset.
  - Ensemble levels aggregation - Aggregate all variables and ensembles into a single dataset, one variable for multiple vertical levels.

You can access the alpha version of the project at https://data.meteo.unican.es/thredds/catalog/devel/EVA/catalog.html.

## Usage

```bash

```
