import sys
import os
import re
import sqlite3
import logging
import time
import zipfile
import pandas as pd
import multiprocessing as mp

from jinja2 import Environment, FileSystemLoader, ChoiceLoader, select_autoescape

PROJECTS = {
  "esgf_dataset": {
    "colname": "eva_esgf_dataset",
    "template": "templates/cmip6_variable.ncml.j2",
    "zipfile": "datasets.zip",
    "dest_replica": "content/public/esgeva/CMIP6/variable/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/replicas/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml",
    "dest_master": "content/public/esgeva/CMIP6/variable/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml",
    "query_dataset": "select * from cmip6 where eva_esgf_dataset = :dataset and opendap != \"\"",
    "query_datasets": "select distinct(eva_esgf_dataset) from cmip6",
  },

  "esgf_ensemble": {
    "colname": "eva_ensemble_aggregation",
    "template": "templates/cmip6notime.ncml.j2",
    "zipfile": "ensembles.zip",
    "dest_replica": "content/public/esgeva/CMIP6/ensemble/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/replicas/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}_{data_node}.ncml",
    "dest_master": "content/public/esgeva/CMIP6/ensemble/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}_{data_node}.ncml",
    "query_dataset": "select * from cmip6 where eva_ensemble_aggregation = :dataset and opendap != \"\"",
    "query_datasets": "select distinct(eva_ensemble_aggregation) from cmip6",
  }
}

class EVAReader(mp.Process):
    def __init__(self, queue, event, dbname):
        super(EVAReader, self).__init__()
        self._queue = queue
        self._event = event
        self._dbname = dbname

    def run(self):
        self.log = logging.getLogger('reader')
        self.log.info("Connecting to DB: %s", self._dbname)

        # submit items to the dataset queue
        conn = sqlite3.connect(self._dbname)
        conn.row_factory = sqlite3.Row

        cursor = conn.cursor()
        try:
            for i,dataset in enumerate(cursor.execute(PROJECTS[project]["query_datasets"])):
                # if i>=50:
                #    break
                path, ncml = generate_ncml(self._dbname, dataset[0])

                if path != "" and ncml != "":
                    self.log.info("Generated: %s", path)
                    self._queue.put((path, ncml))
                    self._event.set()
        finally:
            cursor.close()
            conn.close()

class EVAWriter(mp.Process):
    def __init__(self, queue, event, dest):
        super(EVAWriter, self).__init__()
        self._queue = queue
        self._event = event
        self._dest = dest

    def run(self):
        zf = zipfile.ZipFile(self._dest, "w")

        while self._event.wait( 10.0 ):
            self._event.clear()
            pack = self._queue.get()
            path, ncml = pack[0], pack[1]
            zf.writestr(path, ncml)
        
        zf.close()

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

def generate_ncml(dbname, dataset):
    conn = sqlite3.connect(dbname)
    conn.row_factory = sqlite3.Row

    # columns
    cursor = conn.cursor()
    cursor.execute("select * from cmip6 limit 1")
    item1 = cursor.fetchone()
    columns = pd.MultiIndex.from_tuples( [("GLOBALS", k) for k in item1.keys()] )
    cursor.close()

    # rows
    cursor = conn.cursor()
    cursor.execute(PROJECTS[project]["query_dataset"], {"dataset": dataset})
    df = pd.DataFrame(cursor.fetchall(), columns=columns)
    cursor.close()

    # fixes
    df[("GLOBALS", "version")] = df[("GLOBALS", "id")].str.replace("\|.*", "", regex=True).str.split(".").str[-3]

    # render
    path = ""
    ncml = ""
    if (len(df) > 0) and not (df[("GLOBALS", "opendap")] == "").all():
        if (df[("GLOBALS", "replica")] == 0).all():
            dest = PROJECTS[project]["dest_master"]
        else:
            dest = PROJECTS[project]["dest_replica"]

        d = dict(df['GLOBALS'].iloc[0])
        path = dest.format(**d)
        ncml = template.render({'df': df})

    conn.close()

    return (path, ncml)

if __name__ == "__main__":
    # params
    project = sys.argv[1]
    dbname = "db.sqlite"

    logging.basicConfig(filename="log2", filemode="a", format='%(levelname)10s  %(asctime)s  %(name)10s  %(message)s',level=logging.INFO)

    # jinja
    env = setup_jinja(os.path.dirname(__file__))
    template = env.get_template(os.path.basename(PROJECTS[project]["template"]))

    # multiprocessing
    queue = mp.Queue()

    event = mp.Event()
    ncml_event = mp.Event()

    reader = EVAReader(queue, event, dbname)
    writer = EVAWriter(queue, event, PROJECTS[project]["zipfile"])

    # pool = mp.Pool(3, generate_ncml,(dataset_queue, ncml_queue, event, ncml_event))

    # submit items to the dataset queue
    logging.info("Starting reader.")
    reader.start()
    logging.info("Starting writer.")
    writer.start()

    logging.info("Waiting for reader to finish.")
    reader.join()
    logging.info("Waiting for writer to finish.")
    writer.join()
    # pool.close()
    # pool.join()
