# coding: utf-8

import re
import requests
import json
import h5py

INDEX  = "esgf-node.llnl.gov"
SEARCH = "https://{}/esg-search/search".format(INDEX)
LIMIT  = 10000

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

if __name__ == "__main__":
  # init db
  f = h5py.File('db.h5', 'w')
  cmip6 = f.create_dataset(
    "cmip6",
    (25_000_000,),
    compression="gzip",
    maxshape=(None,),
    dtype=[
      ("id", h5py.string_dtype()),
      ("version", h5py.string_dtype()),
      ("checksum", h5py.string_dtype()),
      ("checksum_type", h5py.string_dtype()),
      ("data_node", h5py.string_dtype()),
      ("index_node", h5py.string_dtype()),
      ("instance_id", h5py.string_dtype()),
      ("master_id", h5py.string_dtype()),
      ("replica", bool),
      ("size", "i8"),
      ("timestamp", h5py.string_dtype()),
      ("title", h5py.string_dtype()),
      ("tracking_id", h5py.string_dtype()),
      ("_timestamp", h5py.string_dtype()),

      ("mip_era", h5py.string_dtype()),
      ("project", h5py.string_dtype()),
      ("institution_id", h5py.string_dtype()),
      ("source_id", h5py.string_dtype()),
      ("experiment_id", h5py.string_dtype()),
      ("table_id", h5py.string_dtype()),
      ("variable_id", h5py.string_dtype()),
      ("grid_label", h5py.string_dtype()),
      ("frequency", h5py.string_dtype()),
      ("realm", h5py.string_dtype()),
      ("product", h5py.string_dtype()),
      ("variant_label", h5py.string_dtype()),
      ("activity_id", h5py.string_dtype()),

      ("further_info_url", h5py.string_dtype()),
      ("pid", h5py.string_dtype()),
      ("opendap", h5py.string_dtype()),

      ("_eva_esgf_dataset", h5py.string_dtype()),
      ("_eva_variable_aggregation", h5py.string_dtype()),
      ("_eva_ensemble_aggregation", h5py.string_dtype())])

  # start searching
  s = requests.Session()
  varnames = cmip6_vars(s)

  payload = {
    "project": "CMIP6",
    "type": "File"
  }

  i = 0
  for varname in varnames:
    payload["variable_id"] = varname
    for record in range_search(s, **payload):
      fixed = cmip6_record(record)
      cmip6[i] = (
        fixed["id"],
        fixed["version"],
        fixed["checksum"],
        fixed["checksum_type"],
        fixed["data_node"],
        fixed["index_node"],
        fixed["instance_id"],
        fixed["master_id"],
        fixed["replica"],
        fixed["size"],
        fixed["timestamp"],
        fixed["title"],
        fixed["tracking_id"],
        fixed["_timestamp"],

        fixed["mip_era"],
        fixed["project"],
        fixed["institution_id"],
        fixed["source_id"],
        fixed["experiment_id"],
        fixed["table_id"],
        fixed["variable_id"],
        fixed["grid_label"],
        fixed["frequency"],
        fixed["realm"],
        fixed["product"],
        fixed["variant_label"],
        fixed["activity_id"],

        fixed["further_info_url"],
        fixed["pid"],
        fixed["opendap"],

        fixed["_eva_esgf_dataset"],
        fixed["_eva_variable_aggregation"],
        fixed["_eva_ensemble_aggregation"])
      i += 1

  s.close()
  f.close()
