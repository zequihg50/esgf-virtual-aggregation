# ESGF Virtual Aggregation

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/zequihg50/eva/HEAD?labpath=demo.ipynb)

The aim of this project is to create a ready-to-deploy TDS catalog including **ALL** available data in ESGF, using
OpenDAP endpoints to provide ESGF data analysis while avoiding the download of any data from remote repositories.

## Usage

The ESGF Virtual Aggregation data workflowo involves two steps:

1 - Query ESGF fedeartion for metadata and store it in a local SQL database.
2 - Generate virtual aggregations (NcMLs) from the SQL database.

ESGF Virtual Aggregation is fully customizable via `selection` files. See the sample file `selection-sample`.

The following code generates the metadata SQL database from the `selection-sample` file.

```bash
python search.py -d sample.db -s selection-sample
```

Now, generate the virtual aggregations (both `esgf_dataset` and `esgf_ensemble`) from the database using 4 parallel jobs.

```bash
python ncmls.py -j4 --database sample.db -p esgf_dataset
python ncmls.py -j4 --database sample.db -p esgf_ensemble
```

You will find that the virtual aggregations are NcML files. You will need a client based on netCDF-java to read them
or you can also set up a TDS server and read via OpenDAP. See next section.

## ESGF Virtual Aggregation demo

Now we will deploy a THREDDS Data Server (TDS) and perform remote data analysis on the ESGF Virtual Aggregation
dataset.

```bash
docker run -p 8080:8080 -v $(pwd)/content:/usr/local/tomcat/content/thredds unidata/thredds-docker:5.0-beta7
```

Now, visit `localhost:8080/thredds` and inspect the server's directory. You may download the NcML from the HTTPServer
endpoint or use the OpenDAP service to get the OpenDAP URL (it should look like `http://localhost:8080/thredds/dodsC/...`).

