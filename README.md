# ESGF Virtual Aggregation

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/zequihg50/eva/HEAD?labpath=demo.ipynb)

Remote data access to Virtual Analysis Ready Data (Virtual ARD) for climate datasets of the [ESGF](https://esgf.llnl.gov/).

Check [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/zequihg50/eva/HEAD?labpath=demo.ipynb), this [Pangeo Showcase](https://discourse.pangeo.io/t/pangeo-showcase-virtual-analysis-ready-data-for-cmip6-and-esgf/4004) or see [run your own ESGF Virtual Aggregation](#run).

## Rationale

The ESGF is a federated file distribution service for climate data. Remote data access and virtual datasets are possible through OPeNDAP and netCDF-java, available by default in all ESGF nodes. However, these capabilities have never been used. This provides:

- Analysis Ready Data (ARD) in the form of virtual datasets, that is, no data duplication needed.
- Remote data access without the need to download files. Open an URL and get direct access to an analytical data cube.

<a id="run"></a>
## Run your own ESGF Virtual Aggregation

The ESGF Virtual Aggregation data involves two steps:

1. Query ESGF fedeartion for metadata and store it in a local SQL database.
2. Generate virtual aggregations (NcMLs) from the SQL database.

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

### Run your own server

A THREDDS Data Server (TDS) with access to the ESGF Virtual Aggregation datasets is available at `https://hub.ipcc.ifca.es/thredds`.

You may deploy your own THREDDS Data Server and perform remote data analysis on the ESGF Virtual Aggregation
dataset.

```bash
docker run -p 8080:8080 -v $(pwd)/content:/usr/local/tomcat/content/thredds unidata/thredds-docker:5.0-beta7
```

Now, visit `localhost:8080/thredds` and inspect the server's directory. You may download the NcML from the HTTPServer
endpoint or use the OpenDAP service to get the OpenDAP URL (it should look like `http://localhost:8080/thredds/dodsC/...`).

The OpenDAP service may be used to perform remote data analysis using xarray.

```python
import xarray,dask

dask.config.set(scheduler="processes")

url = "http://localhost:8080/thredds/dodsC/esgeva/demo/CMIP6_CMIP_AS-RCEC_TaiESM1_historical_day_tas_gn_v20200626_esgf.ceda.ac.uk.ncml"
ds = xarray.open_dataset(url).chunk({"time": 100})

# query the size of the dataset on the server side
ds.attrs["size_human"]

# view the variant_label coordinate
ds["variant_label"][...].compute()

# compute spatial mean for all variant_labels
# this involves transferring the necessary data from the server
means = ds["tas"].mean(["lat", "lon"]).compute()
means
```

See the notebooks for usage and reproducibility.
