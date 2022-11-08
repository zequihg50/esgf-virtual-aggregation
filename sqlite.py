# coding: utf-8

import os
import sys
import re
import requests
import json

import sqlite3, h5py
import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader, ChoiceLoader, select_autoescape

import requests
from pydap.client import open_url
from multiprocessing import Pool

INDEX  = "esgf-node.llnl.gov"
SEARCH = "https://{}/esg-search/search".format(INDEX)
LIMIT  = 10000

HDF5DB = "coordsdb.h5"

CONFIG = {
  "cmip6": {
    "fieldscore": ("id",
                   "version",
                   "checksum",
                   "checksum_type",
                   "data_node",
                   "index_node",
                   "instance_id",
                   "master_id",
                   "replica",
                   "size",
                   "timestamp",
                   "title",
                   "tracking_id",
                   "_timestamp"),
    "fieldsesgfproject": ("mip_era",
                          "project",
                          "institution_id",
                          "source_id",
                          "experiment_id",
                          "table_id",
                          "variable_id",
                          "grid_label",
                          "frequency",
                          "realm",
                          "product",
                          "variant_label",
                          "further_info_url",
                          "activity_id",
                          "pid"),
    "eva": {
        "ensemble": ("mip_era", "activity_id", "institution_id", "source_id", "experiment_id",
                     "table_id", "realm", "grid_label", "version"),
    },
    "fieldsensemble": ("mip_era", "activity_id", "institution_id", "source_id", "experiment_id", "table_id", "grid_label", "realm"),
  },
  "cordex": {
    "fieldscore": ("id", "version", "checksum", "checksum_type", "data_node", "index_node", "instance_id",
                   "master_id", "replica", "size", "timestamp", "title", "tracking_id", "_timestamp", "opendap"),
    "fieldsesgfproject": ("domain", "driving_model", "ensemble", "experiment", "institute", "product", "project",
                          "rcm_name", "rcm_version", "time_frequency", "variable"),
    "fieldsunlist": ("checksum", "checksum_type", "domain", "driving_model", "experiment", "ensemble",
                     "institute", "product", "project", "rcm_name", "rcm_version", "time_frequency",
                     "tracking_id", "variable"),
  },
}

def find_opendap_url(urls):
  for url in urls:
    if url.endswith("OPENDAP"):
      return url

  return ""

def cmip6_vars(session):
  r = session.get(SEARCH, params={
    "facets": "variable_id",
    "project": "CMIP6",
    "limit": 0,
    "format": "application/solr+json"})

  return r.json()["facet_counts"]["facet_fields"]["variable_id"][::2]

def cmip6_version_datasetid(dataset_id):
  version = re.sub("\|.*", "", dataset_id)
  version = re.sub(".*\.", "", version)

  return version

def cmip6_record(record):
  row = {}

  for field in CONFIG["cmip6"]["fieldscore"]:
    if field not in record:
      #raise Exception("Missing core field in record *{}*".format(field))
      row[field] = ""
    elif isinstance(record[field], list):
      row[field] = record[field][0]
    else:
      row[field] = record[field]

  for field in CONFIG["cmip6"]["fieldsesgfproject"]:
    if field not in record:
      raise Exception("Missing CMIP6 field in record *{}*".format(field))

    if isinstance(record[field], list):
      row[field] = record[field][0]

  try:
    opendap = find_opendap_url(record["url"])
    opendap = re.sub("\|.*", "", opendap)
    opendap = re.sub("\.html$", "", opendap)
    row["opendap"] = opendap
  except:
    raise Exception("Error while obtaining OPENDAP URL")

  row["_eva_esgf_dataset"] = "_".join([
    record["project"][0],
    record["activity_id"][0],
    record["institution_id"][0],
    record["source_id"][0],
    record["experiment_id"][0],
    record["member_id"][0],
    record["table_id"][0],
    record["variable_id"][0],
    record["grid_label"][0],
    cmip6_version_datasetid(record["dataset_id"]),
    record["data_node"]])

  row["_eva_variable_aggregation"] = "_".join([
    record["project"][0],
    record["activity_id"][0],
    record["institution_id"][0],
    record["source_id"][0],
    record["experiment_id"][0],
    record["member_id"][0],
    record["table_id"][0],
    record["grid_label"][0],
    cmip6_version_datasetid(record["dataset_id"]),
    record["data_node"]])

  row["_eva_ensemble_aggregation"] = "_".join([
    record["project"][0],
    record["activity_id"][0],
    record["institution_id"][0],
    record["source_id"][0],
    record["experiment_id"][0],
    record["table_id"][0],
    record["grid_label"][0],
    cmip6_version_datasetid(record["dataset_id"]),
    record["data_node"]])

  return row

def range_search(session, stop=None, **kwargs):
  payload = kwargs

  # how many records?
  payload["limit"] = 0
  payload["format"] = "application/solr+json"
  n = session.get(SEARCH, params=payload).json()["response"]["numFound"]

  # if stop < n, restrict
  if stop is not None and stop < n:
    n = stop

  # clean payload and start searching
  payload = kwargs
  i = 0
  while i < n:
    payload["limit"]  = LIMIT
    payload["format"] = "application/solr+json"
    payload["offset"] = i

    r = session.get(SEARCH, params=payload)
    print(r.url, flush=True)
    for f in r.json()["response"]["docs"]:
      yield f

    i = i + LIMIT

####################################################################################################
############################################# DataFrame ############################################
####################################################################################################
def read_nc_remote(frm, session):
  attrs = {}
  ds = open_url(frm, session=session)

#  for attr in ds.attributes["NC_GLOBAL"]:
#    attrs[("GLOBALS", attr)] = ds.attributes["NC_GLOBAL"][attr]

  for variable in ds:
      dimensions = ds[variable].dimensions
      attrs[(variable, '_dimensions')] = ','.join(dimensions)
      attrs[(variable, "_shape")] = ','.join([str(x) for x in ds[variable].shape])
      for attr in ds[variable].attributes:
          attrs[(variable, attr)] = ds[variable].attributes[attr]

      # Read coordinate values
      if len(dimensions) < 2:
          a = np.array(ds[variable])
          attrs[(variable, "_values")] = a

      # Read dimensions
      dims = ds[variable].dimensions
      shape = ds[variable].shape
      for dname,dshape in zip(dims, shape):
        attrs[('_'.join(['_d', dname]), 'name')] = dname
        attrs[('_'.join(['_d', dname]), 'size')] = dshape

  return attrs

def read_nc_hdf5(frm, names, db):
  pass

def get_hdf5id(facets):
  return "{}/{}/{}/{}/{}/{}/{}/{}/{}/db.h5".format(
    facets["data_node"],
    facets["project"],
    facets["activity_id"],
    facets["institution_id"],
    facets["source_id"],
    facets["experiment_id"],
    facets["table_id"],
    facets["grid_label"],
    facets["version"])

def readvariables(df):
  session = requests.Session()

  #variables_df = df.apply(lambda row: _readvariables(row[('GLOBALS', 'opendap')], ["time"], session), axis=1)
  variables_df = []
  for index,row in df.iterrows():
    hdf5id = get_hdf5id(row["GLOBALS"])
    hdf5db_path = os.path.join("hdf5db", hdf5id)
    os.makedirs(os.path.dirname(hdf5db_path), exist_ok=True)
    hdf5db = h5py.File(hdf5db_path, "a")

    if row[("GLOBALS", "title")] not in hdf5db:
      group = hdf5db.create_group(row[("GLOBALS", "title")])
    else:
      group = hdf5db[row[("GLOBALS", "title")]]

    # use local db or query server
    if False: # need to check timestamps too
      pass
    else:
      data = read_nc_remote(row[('GLOBALS', 'opendap')], session)
      variables_df.append(data)

      # write to hdf5db
      for key in data:
        name = key[0]
        if name == "GLOBALS":
          group.attrs[key[1]] = data[key]
        else:
          if (name not in group) and ((name, "_values") in data) and (len(data[(name, "_values")].shape) > 0):
            shape = data[(name, "_values")].shape
            dtype = data[(name, "_values")].dtype
            dset = group.create_dataset(name, shape, dtype=dtype, chunks=True, compression="gzip")
            dset[:] = data[(name, "_values")][:]
          elif name not in group:
            dset = group.create_dataset(name, dtype="f") # scalar datasets, just to store attributes
          else:
            dset = group[name]
  
          if key[1] != "_values":
            dset.attrs[key[1]] = data[key]

    hdf5db.close()
  session.close()

  variables_df = pd.DataFrame.from_records(variables_df)
  variables_df.columns = pd.MultiIndex.from_tuples(variables_df.columns)

  return variables_df

####################################################################################################
############################################# JINJA ###############################################
####################################################################################################
def f_timeunitschange(df, timecol=None, units=None, calendar=None):
    if timecol is None:
        timecol = 'time'
    if units is None:
        units = df[(timecol, 'units')].iloc[0]
    if calendar is None:
        calendar = df[(timecol, 'calendar')].iloc[0]

    df[(timecol, '_values')] = df.apply(lambda row:
        cftime.num2date(row[(timecol, '_values')], row[(timecol, 'units')], row[(timecol, 'calendar')]), axis=1)
    df[(timecol, '_values')] = df.apply(lambda row:
        cftime.date2num(row[(timecol, '_values')], units, calendar), axis=1)

    return df

def setup_jinja(templates):
    default_templates = os.path.join(os.path.dirname(__file__), 'templates')
    loader = ChoiceLoader([
        FileSystemLoader(templates),
        FileSystemLoader(os.getcwd()),
        FileSystemLoader(default_templates),
    ])

    env = Environment(
        loader=loader,
        autoescape=select_autoescape(['xml']),
        trim_blocks=True,
        lstrip_blocks=True)

    env.filters['basename'] = lambda path: os.path.basename(path)
    env.filters['dirname'] = lambda path: os.path.dirname(path)
    env.filters['regex_replace'] = lambda s, find, replace: re.sub(find, replace, s)
    env.filters['timeunitschange'] = f_timeunitschange

    env.tests['onestep'] = lambda arr: len(np.unique(np.diff(arr))) == 1

    return env

def render(df, dest, **kwargs):
  d = dict(df['GLOBALS'].iloc[0])
  path = os.path.abspath(dest.format(**d))

  os.makedirs(os.path.dirname(path), exist_ok=True)
  with open(path, 'w+') as fh:
    fh.write(template.render({**kwargs, 'df': df}))

  print(path, flush=True)

####################################################################################################
############################################# MAIN #################################################
####################################################################################################
def generate_ncml(dataset):
  conn = sqlite3.connect("db.sqlite")
  conn.row_factory = sqlite3.Row

  dataset_items = conn.cursor()
  dataset_items.execute("select * from cmip6 where eva_ensemble_aggregation = :dataset", {"dataset": dataset})
  item1 = dataset_items.fetchone()
  columns = pd.MultiIndex.from_tuples( [("GLOBALS", k) for k in item1.keys()] )

  df = pd.DataFrame(dataset_items.fetchall(), columns=columns)
  if len(df) > 0:
    df = pd.concat([df, readvariables(df)], axis=1)
    render(df, dest)

  dataset_items.close()
  conn.close()

def datasets_for_datanode(datanode):
  try:
    conn = sqlite3.connect("db.sqlite")
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
  
    for dataset in cursor.execute("select eva_ensemble_aggregation from cmip6 where data_node = :datanode", {"datanode": datanode}):
      generate_ncml(dataset[0])
  except:
    print("Error for datanode: {}".format(datanode))
    print(sys.exc_info())
  finally:
    cursor.close()
    conn.close()

if __name__ == "__main__":
  # init db
  # create sqlite db and get connection

#  # start searching
#  s = requests.Session()
#  varnames = cmip6_vars(s)
#
#  payload = {
#    "project": "CMIP6",
#    "type": "File"
#  }
#
#  i = 0
#  for varname in varnames:
#    payload["variable_id"] = varname
#    for record in range_search(s, **payload):
#      fixed = cmip6_record(record)
#      newrow = (
#        fixed["id"],
#        fixed["version"],
#        fixed["checksum"],
#        fixed["checksum_type"],
#        fixed["data_node"],
#        fixed["index_node"],
#        fixed["instance_id"],
#        fixed["master_id"],
#        fixed["replica"],
#        fixed["size"],
#        fixed["timestamp"],
#        fixed["title"],
#        fixed["tracking_id"],
#        fixed["_timestamp"],
#
#        fixed["mip_era"],
#        fixed["project"],
#        fixed["institution_id"],
#        fixed["source_id"],
#        fixed["experiment_id"],
#        fixed["table_id"],
#        fixed["variable_id"],
#        fixed["grid_label"],
#        fixed["frequency"],
#        fixed["realm"],
#        fixed["product"],
#        fixed["variant_label"],
#        fixed["activity_id"],
#
#        fixed["further_info_url"],
#        fixed["pid"],
#        fixed["opendap"],
#
#        fixed["_eva_esgf_dataset"],
#        fixed["_eva_variable_aggregation"],
#        fixed["_eva_ensemble_aggregation"])
#      # insert new row
#      i += 1
#
#  s.close()

  # generate index

  # start ncmls
  env = setup_jinja(os.path.dirname(__file__))
  #template = env.get_template(os.path.basename("templates/cmip6notime.ncml.j2"))
  template = env.get_template(os.path.basename("templates/cmip6.ncml.j2"))

  conn = sqlite3.connect("db.sqlite")
  conn.row_factory = sqlite3.Row

  cursor = conn.cursor()
  datanodes = [x[0] for x in cursor.execute("select distinct(data_node) from cmip6").fetchall()]

  datasets = []
  for dataset in conn.cursor().execute("select distinct(eva_ensemble_aggregation) from cmip6").fetchall():
    datasets.append(dataset[0])
  datasets = list(filter(lambda x: "_day_" in x, datasets))

  cursor.close()
  conn.close()

  dest = "ncmls/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_v{version}_{data_node}.ncml"
  with Pool(6) as p:
    p.map(datasets_for_datanode, datanodes)
