import argparse
import logging
import re
import sqlite3
import sys
import requests

INDEX_NODES = [
    "esg-dn1.nsc.liu.se",
    "esgf-node.llnl.gov",
    "esgf.nci.org.au",
    "esgf-data.dkrz.de",
    "esgf-index1.ceda.ac.uk",
    "esgf-node.ipsl.upmc.fr",
]

INDEX = INDEX_NODES[0]
SEARCH = "https://{}/esg-search/search".format(INDEX)
LIMIT = 9000
TIMEOUT = 120  # seconds
TOLERANCE = 12  # if each node (6 index nodes) fails two times, abort


class Project():
    def find_opendap_url(self, urls):
        opendap_url = ""
        for url in urls:
            if url.endswith("OPENDAP"):
                opendap_url = url

        return opendap_url

    def parse_record(self, record):
        raise NotImplementedError

    def get_datanode_variable_pairs(self):
        raise NotImplementedError

    def get_version(self):
        raise NotImplementedError


class Cmip6(Project):
    def __init__(self):
        super().__init__()
        self.core = ("id", "version", "checksum", "checksum_type", "data_node", "index_node", "instance_id",
                     "master_id", "replica", "size", "timestamp", "title", "tracking_id", "_timestamp")
        self.project = ("mip_era", "project", "institution_id", "source_id", "experiment_id", "table_id",
                        "variable_id", "grid_label", "frequency", "realm", "product", "variant_label",
                        "further_info_url", "activity_id", "pid")

    def get_datanode_variable_pairs(self):
        session = requests.Session()
        r = session.get(SEARCH, timeout=TIMEOUT, params={
            "facets": "variable_id,data_node",
            "project": "CMIP6",
            "limit": 0,
            "format": "application/solr+json"})
        response = r.json()
        session.close()

        variables = response["facet_counts"]["facet_fields"]["variable_id"][::2]
        datanodes = response["facet_counts"]["facet_fields"]["data_node"][::2]

        facets = [{"data_node": datanode, "variable_id": variable}
                  for datanode in datanodes
                  for variable in variables]

        return facets

    def get_version(self, dataset_id):
        version = re.sub("\|.*", "", dataset_id)
        version = re.sub(".*\.", "", version)

        return version

    def parse_record(self, record):
        row = {}

        for field in self.core:
            if field not in record:
                logging.error(
                    "Missing CORE field: {} in {}.".format(field, record))
                row[field] = ""
            elif isinstance(record[field], list):
                row[field] = record[field][0]
            else:
                row[field] = record[field]

        for field in self.project:
            if field not in record:
                row[field] = ""
                logging.error(
                    "Missing PROJECT field: {} in {}.".format(field, record))
            elif isinstance(record[field], list):
                row[field] = record[field][0]
            else:
                row[field] = record[field]

        row["opendap"] = ""
        try:
            opendap = self.find_opendap_url(record["url"])
            opendap = re.sub("\|.*", "", opendap)
            opendap = re.sub("\.html$", "", opendap)
            row["opendap"] = opendap
        except:
            logging.error("Missing OPENDAP field in {}.".format(record))

        row["eva_esgf_dataset"] = "_".join([
            record["project"][0],
            record["activity_id"][0],
            record["institution_id"][0],
            record["source_id"][0],
            record["experiment_id"][0],
            record["variant_label"][0],
            record["table_id"][0],
            record["variable_id"][0],
            record["grid_label"][0],
            self.get_version(record["dataset_id"]),
            record["data_node"]])

        row["eva_ensemble_aggregation"] = "_".join([
            record["project"][0],
            record["activity_id"][0],
            record["institution_id"][0],
            record["source_id"][0],
            record["experiment_id"][0],
            record["table_id"][0],
            record["variable_id"][0],
            record["grid_label"][0],
            self.get_version(record["dataset_id"]),
            record["data_node"]])

        return row


def range_search(session, stop=None, **kwargs):
    payload = kwargs

    # how many records?
    payload["limit"] = 0
    payload["format"] = "application/solr+json"
    n = session.get(SEARCH,
                    params=payload,
                    timeout=TIMEOUT
                    ).json()["response"]["numFound"]

    # if stop < n, restrict
    if stop is not None and stop < n:
        n = stop

    # clean payload and start searching
    payload = kwargs
    i = 0
    while i < n:
        payload["limit"] = LIMIT
        payload["format"] = "application/solr+json"
        payload["offset"] = i

        r = session.get(SEARCH, params=payload, timeout=TIMEOUT)
        print(r.url, flush=True)
        for f in r.json()["response"]["docs"]:
            yield f

        i = i + LIMIT


def createdb(cursor):
    cursor.execute("DROP TABLE IF EXISTS cmip6")
    cursor.execute("""CREATE TABLE cmip6 (
    id TEXT,
    version TEXT,
    checksum TEXT,
    checksum_type VARCHAR(10),
    data_node TEXT,
    index_node TEXT,
    instance_id TEXT,
    master_id TEXT,
    replica INTEGER,
    size UNSIGNED BIG INT,
    timestamp TEXT,
    title TEXT,
    tracking_id TEXT,
    _timestamp TEXT,
    
    mip_era TEXT,
    project TEXT,
    institution_id TEXT,
    source_id TEXT,
    experiment_id TEXT,
    table_id TEXT,
    variable_id TEXT,
    grid_label TEXT,
    frequency TEXT,
    realm TEXT,
    product TEXT,
    variant_label TEXT,
    activity_id TEXT,
    
    
    further_info_url TEXT,
    pid TEXT,
    opendap TEXT,
    
    eva_esgf_dataset TEXT,
    eva_ensemble_aggregation TEXT)""")


def search(session, project, query):
    payload = {
        "project": "CMIP6",
        "type": "File",
        "distrib": "True"
    }

    params = payload.copy()
    params.update(query)
    for record in range_search(session, **params):
        fixed = project.parse_record(record)
        newrow = (
            fixed["id"],
            fixed["version"],
            fixed["checksum"],
            fixed["checksum_type"],
            fixed["data_node"],
            fixed["index_node"],
            fixed["instance_id"],
            fixed["master_id"],
            str(1 if fixed["replica"] else 0),
            str(fixed["size"]),
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
            fixed["eva_esgf_dataset"],
            fixed["eva_ensemble_aggregation"]
        )

        yield newrow


class Query:
    def query(self):
        raise NotImplementedError


class ExtensiveQuery(Query):
    def __init__(self, project):
        self.project = project

    def query(self):
        for q in project.get_datanode_variable_pairs():
            yield q


class SelectionQuery(Query):
    def __init__(self, selection):
        self.selection = selection

    def query(self):
        for x in self.parse_selection(self.selection):
            yield x

    def parse_selection(self, sfile):
        q = {}
        with open(sfile, "r") as f:
            for line in f:
                if line == "\n" and q:
                    yield q
                    q = {}
                elif line != "\n":
                    params = line.rstrip("\n").split()
                    for param in params:
                        k = param.split("=")[0]
                        v = param.split("=")[1]
                        q[k] = v

            if q:
                yield q


if __name__ == "__main__":
    # arguments
    parser = argparse.ArgumentParser(description="Query ESGF files and store results in sqlite.")
    parser.add_argument("-d", "--dest",
                        type=str,
                        required=False,
                        default="db.sqlite",
                        help="destination file.")
    parser.add_argument("-p", "--project",
                        choices=["CMIP6"],
                        default="CMIP6",
                        type=str,
                        required=False,
                        help="ESGF project.")
    parser.add_argument("-l", "--log-file",
                        type=str,
                        required=False,
                        default="search.log",
                        help="log file.")
    parser.add_argument("-s", "--selection",
                        type=str,
                        required=False,
                        default=None,
                        help="selection file.")
    parser.set_defaults()
    args = vars(parser.parse_args())

    logging.basicConfig(filename=args["log_file"],
                        encoding="utf-8",
                        level=logging.DEBUG)

    if args["project"] == "CMIP6":
        project = Cmip6()

    if args["selection"]:
        query = SelectionQuery(args["selection"])
    else:
        query = ExtensiveQuery(project)

    # create sqlite db and get connection
    logging.info("Set up sqlite database.")
    conn = sqlite3.connect(args["dest"])
    c = conn.cursor()
    createdb(c)

    # start searching
    logging.info("Create Session: {}".format(INDEX))
    s = requests.Session()

    try:
        i = 0
        for q in query.query():
            inserted = False
            tolerance = 0
            while not inserted:  # loop index nodes while failing
                try:
                    conn.execute("begin")
                    for row in search(s, project, q):
                        c.execute(
                            "INSERT INTO cmip6 VALUES({})".format(
                                ",".join(["?" for _ in range(len(row))])),
                            row)
                    conn.commit()
                    inserted = True
                except:
                    conn.execute("rollback")
                    logging.exception(
                        "Failed while retrieving {}.".format(q))
                    s.close()

                    # too many errors?
                    tolerance += 1
                    if tolerance >= TOLERANCE:
                        sys.exit(2)

                    # use new index
                    i = (i + 1) % len(INDEX_NODES)
                    INDEX = INDEX_NODES[i]
                    SEARCH = "https://{}/esg-search/search".format(INDEX)
                    s = requests.Session()
                    logging.info("Changing INDEX to: {}.".format(INDEX))
        s.close()
    finally:
        c.execute(
            "CREATE INDEX cmip6_data_node ON cmip6(data_node)")
        c.execute(
            "CREATE INDEX cmip6_eva_esgf_dataset ON cmip6(eva_esgf_dataset)")
        c.execute(
            "CREATE INDEX cmip6_eva_ensemble_aggregation ON cmip6(eva_ensemble_aggregation)")

        c.close()
        conn.close()
