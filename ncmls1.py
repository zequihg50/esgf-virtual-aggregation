# coding: utf-8

import os
import sys
import re
import logging

import zipfile
import sqlite3
import numpy as np
import pandas as pd
from jinja2 import Environment, FileSystemLoader, ChoiceLoader, select_autoescape

from multiprocessing import Pool

PROJECTS = {
  "esgf_dataset": {
    "template": "templates/cmip6_variable.ncml.j2",
    "zipfile": "esgf_dataset.zip",
    "dest_replica": "content/public/esgeva/CMIP6/variable/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/replicas/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml",
    "dest_master": "content/public/esgeva/CMIP6/variable/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml",
    "query_dataset": "select * from cmip6 where eva_esgf_dataset = :dataset and opendap != \"\"",
    "query_datasets": "select distinct(eva_esgf_dataset) from cmip6",
  },

  "esgf_dataset_test": {
    "template": "templates/cmip6_variable.ncml.j2",
    "zipfile": "esgf_dataset_test.zip",
    "dest_replica": "content/public/esgeva/CMIP6/variable/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/replicas/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml",
    "dest_master": "content/public/esgeva/CMIP6/variable/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml",
    "query_dataset": "select * from cmip6 where eva_esgf_dataset = :dataset and opendap != \"\"",
    "query_datasets": "select distinct(eva_esgf_dataset) from cmip6 limit 100",
  },

  "esgf_ensemble": {
    "template": "templates/cmip6_ensemble.ncml.j2",
    "zipfile": "esgf_ensemble.zip",
    "dest_replica": "content/public/esgeva/CMIP6/ensemble/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/replicas/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}_{data_node}.ncml",
    "dest_master": "content/public/esgeva/CMIP6/ensemble/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}_{data_node}.ncml",
    "query_dataset": "select * from cmip6 where eva_ensemble_aggregation = :dataset and opendap != \"\"",
    "query_datasets": "select distinct(eva_ensemble_aggregation) from cmip6",
  },

  "esgf_ensemble_test": {
    "template": "templates/cmip6_ensemble.ncml.j2",
    "zipfile": "esgf_ensemble_test.zip",
    "dest_replica": "content/public/esgeva/CMIP6/ensemble/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/replicas/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}_{data_node}.ncml",
    "dest_master": "content/public/esgeva/CMIP6/ensemble/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}_{data_node}.ncml",
    "query_dataset": "select * from cmip6 where eva_ensemble_aggregation = :dataset and opendap != \"\"",
    "query_datasets": "select distinct(eva_ensemble_aggregation) from cmip6 limit 10000",
  },
}

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

    return env

def generate_ncml(zf, conn, dataset):
  dataset_items = conn.cursor()
  dataset_items.execute("select * from cmip6 limit 1")
  item1 = dataset_items.fetchone()
  columns = pd.MultiIndex.from_tuples( [("GLOBALS", k) for k in item1.keys()] )
  dataset_items.close()

  dataset_items = conn.cursor()
  dataset_items.execute(PROJECTS[project]["query_dataset"], {"dataset": dataset})
  df = pd.DataFrame(dataset_items.fetchall(), columns=columns)
  df[("GLOBALS", "version")] = df[("GLOBALS", "id")].str.replace("\|.*", "", regex=True).str.split(".").str[-3]
  if len(df) > 0:
    if (df[("GLOBALS", "replica")] == 0).all():
      dest = PROJECTS[project]["dest_master"]
    else:
      dest = PROJECTS[project]["dest_replica"]
    #render(df, dest)
    d = dict(df['GLOBALS'].iloc[0])
    path = dest.format(**d)
    data = template.render({'df': df})
    zf.writestr(path, data)
    print(path, flush=True)

  dataset_items.close()

if __name__ == "__main__":
  if sys.argv[1] is None:
    print("Please, specify a project: esgf_dataset, esgf_ensemble.", file=sys.stderr)
    sys.exit(1)
  project = sys.argv[1]

  # start ncmls
  env = setup_jinja(os.path.dirname(__file__))
  template = env.get_template(os.path.basename(PROJECTS[project]["template"]))

  conn = sqlite3.connect("db.sqlite")
  conn.row_factory = sqlite3.Row

  cursor = conn.cursor()
  datasets = cursor.execute(PROJECTS[project]["query_datasets"])

  with zipfile.ZipFile(PROJECTS[project]["zipfile"], "w") as f:
    for dataset in datasets:
      data = generate_ncml(f, conn, dataset[0])

  cursor.close()
  conn.close()
