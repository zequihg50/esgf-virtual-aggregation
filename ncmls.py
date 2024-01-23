import argparse
import os
import re
import sqlite3
import pandas as pd
from multiprocessing import Pool
from jinja2 import Environment, FileSystemLoader, ChoiceLoader, select_autoescape


class Project:
    def template(self):
        raise NotImplementedError

    def query_dataset(self):
        raise NotImplementedError

    def query_datasets(self):
        raise NotImplementedError

    def dest_master(self, df):
        raise NotImplementedError

    def dest_replica(self, df):
        raise NotImplementedError


class CMIP6Dataset(Project):
    def __init__(self):
        self._template = "templates/esgf_dataset.ncml.j2"
        self._query_dataset = "select * from cmip6 where eva_esgf_dataset = :dataset and opendap != \"\""
        self._query_datasets = "select distinct(eva_esgf_dataset) from cmip6"
        self._dest_master = "content/thredds/public/esgeva/variable/CMIP6/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{member_id}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml"
        self._dest_replica = "content/thredds/public/esgeva/variable/CMIP6/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/replicas/{data_node}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{member_id}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml"


class CMIP6Ensemble(Project):
    def __init__(self):
        self._template = "templates/esgf_ensemble.ncml.j2"
        self._query_dataset = "select * from cmip6 where eva_ensemble_aggregation = :dataset and opendap != \"\""
        self._query_datasets = "select distinct(eva_ensemble_aggregation) from cmip6"

    @property
    def template(self):
        return self._template

    @property
    def query_dataset(self):
        return self._query_dataset

    @property
    def query_datasets(self):
        return self._query_datasets

    def dest_master(self, df):
        if (df["sub_experiment_id"] == "none").any():
            return "content/thredds/public/esgeva/ensemble/CMIP6/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{variable_id}_{grid_label}_{version}.ncml"
        else:
            return "content/thredds/public/esgeva/ensemble/CMIP6/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{sub_experiment_id}_{table_id}_{variable_id}_{grid_label}_{version}.ncml"

    def dest_replica(self, df):
        if (df["sub_experiment_id"] == "none").any():
            return "content/thredds/public/esgeva/ensemble/CMIP6/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/replicas/{data_node}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{variable_id}_{grid_label}_{version}.ncml"
        else:
            return "content/thredds/public/esgeva/ensemble/CMIP6/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/replicas/{data_node}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{sub_experiment_id}_{table_id}_{variable_id}_{grid_label}_{version}.ncml"


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


def generate_ncml(dataset):
    conn = get_conn(db)
    dataset_items = conn.cursor()
    dataset_items.execute("select * from cmip6 limit 1")
    item1 = dataset_items.fetchone()
    columns = [k for k in item1.keys()]
    dataset_items.close()

    dataset_items = conn.cursor()
    dataset_items.execute(project.query_dataset, {"dataset": dataset})
    df = pd.DataFrame(dataset_items.fetchall(), columns=columns)
    df["version"] = df["id"].str.replace("\|.*", "", regex=True).str.split(".").str[-3]
    if len(df) > 0:
        if (df["replica"] == 0).all():
            dest = project.dest_master(df)
        else:
            dest = project.dest_replica(df)

        d = dict(df.iloc[0])
        path = dest.format(**d)
        abspath = os.path.abspath(path)
        data = template.render({'df': df})

        os.makedirs(os.path.dirname(abspath), exist_ok=True)
        with open(abspath, 'w+') as fh:
            fh.write(data)

        print(abspath, flush=True)

    dataset_items.close()
    conn.close()


def get_conn(db):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row

    return conn


def init_worker(d, p):
    global db, project
    db = d
    project = p


if __name__ == "__main__":
    # arguments
    parser = argparse.ArgumentParser(description="Query ESGF files and store results in sqlite.")
    parser.add_argument("-p", "--project",
                        choices=["esgf_dataset", "esgf_ensemble"],
                        type=str,
                        required=False,
                        default="esgf_ensemble",
                        help="type of ESGF Virtual Aggregation.")
    parser.add_argument("--database",
                        required=True,
                        type=str,
                        help="database file.")
    #    parser.add_argument("-d", "--dest",
    #                        type=str,
    #                        required=False,
    #                        default="",
    #                        help="destination directory.")
    parser.add_argument("-j", "--jobs",
                        type=int,
                        required=False,
                        default=8,
                        help="number of jobs.")
    parser.set_defaults()
    args = vars(parser.parse_args())

    # project
    project = CMIP6Ensemble()

    # start ncmls
    env = setup_jinja(os.path.dirname(__file__))
    template = env.get_template(os.path.basename(project.template))

    conn = sqlite3.connect(args["database"])
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    datasets = [d[0] for d in cursor.execute(project.query_datasets).fetchall()]

    cursor.close()
    conn.close()

    with Pool(
            args["jobs"],
            initializer=init_worker,
            initargs=(args["database"], project)
    ) as pool:
        pool.map(generate_ncml, datasets)
