# ESGF Virtual Aggregation

The aim of this project is to create a ready-to-deploy TDS catalog including **ALL** available data in ESGF, using OpenDAP endpoints to provide ESGF data analysis while avoiding the download of any data from remote repositories.

**Why?** Because we think that currently ESGF does not make use of state of the art capabilities in their current software stack that would easily provide additional useful services for end users.

The EVA works as follows:

1 - Use ESGF search service to query available information.

2 - Store file metadata locally using SQL to efficiently perform queries.

3 - Create THREDDS NcMLs and catalogs using OpenDAP endpoints.

4 - Provide multiple virtual views of the datasets:
  - ESGF dataset - Aggregate time series for variables.
  - ESGF ensemble - Aggregate time series and ensembles into a single dataset.

## Usage

## Notes

Check facets from ESGF: `https://esgf-node.llnl.gov/esg-search/search/?limit=0&type=File&project=CMIP6&format=application%2Fsolr%2Bjson&facets=*`

Check facets from EVA: `select activity_id, count(activity_id) from cmip6 group by activity_id`

## Contact
