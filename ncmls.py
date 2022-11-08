import argparse
import os
import re
import sqlite3
import pandas as pd
from multiprocessing import Pool
from jinja2 import Environment, FileSystemLoader, ChoiceLoader, select_autoescape

PROJECTS = {
    "esgf_dataset": {
        "template": "templates/esgf_dataset.ncml.j2",
        "dest_replica": "esgeva/CMIP6/variable/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/replicas/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml",
        "dest_master": "esgeva/CMIP6/variable/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{variant_label}_{table_id}_{variable_id}_{grid_label}_{version}_{data_node}.ncml",
        "query_dataset": "select * from cmip6 where eva_esgf_dataset = :dataset and opendap != \"\"",
        "query_datasets": "select distinct(eva_esgf_dataset) from cmip6",
    },

    "esgf_ensemble": {
        "template": "templates/esgf_ensemble.ncml.j2",
        "dest_replica": "esgeva/CMIP6/ensemble/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/replicas/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}_{data_node}.ncml",
        "dest_master": "esgeva/CMIP6/ensemble/{activity_id}/{table_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{grid_label}_{version}_{data_node}.ncml",
        "query_dataset": "select * from cmip6 where eva_ensemble_aggregation = :dataset and opendap != \"\"",
        "query_datasets": "select distinct(eva_ensemble_aggregation) from cmip6",
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


def generate_ncml(dataset):
    conn = get_conn(db)
    dataset_items = conn.cursor()
    dataset_items.execute("select * from cmip6 limit 1")
    item1 = dataset_items.fetchone()
    columns = [k for k in item1.keys()]
    dataset_items.close()

    dataset_items = conn.cursor()
    dataset_items.execute(PROJECTS[project]["query_dataset"], {"dataset": dataset})
    df = pd.DataFrame(dataset_items.fetchall(), columns=columns)
    df["version"] = df["id"].str.replace("\|.*", "", regex=True).str.split(".").str[-3]
    if len(df) > 0:
        if (df["replica"] == 0).all():
            dest = PROJECTS[project]["dest_master"]
        else:
            dest = PROJECTS[project]["dest_replica"]

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
                        default="esgf_dataset",
                        help="type of ESGF Virtual Aggregation.")
    parser.add_argument("--database",
                        required=True,
                        type=str,
                        help="database file.")
    parser.add_argument("-d", "--dest",
                        type=str,
                        required=False,
                        default="",
                        help="destination directory.")
    parser.add_argument("-j", "--jobs",
                        type=int,
                        required=False,
                        default=8,
                        help="number of jobs.")
    parser.set_defaults()
    args = vars(parser.parse_args())

    # start ncmls
    env = setup_jinja(os.path.dirname(__file__))
    template = env.get_template(os.path.basename(PROJECTS[args["project"]]["template"]))

    conn = sqlite3.connect(args["database"])
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    datasets = [d[0] for d in cursor.execute(PROJECTS[args["project"]]["query_datasets"]).fetchall()]

    cursor.close()
    conn.close()

    with Pool(
            args["jobs"],
            initializer=init_worker,
            initargs=(
                    args["database"],
                    args["project"],)
    ) as pool:
        pool.map(generate_ncml, datasets)
