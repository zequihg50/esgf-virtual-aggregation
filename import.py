import json
import psycopg2

config = {
    "cmip6": {
        "fieldscore": ("id", "version", "checksum", "checksum_type", "data_node", "index_node", "instance_id",
                       "master_id", "replica", "size", "timestamp", "title", "tracking_id", "_timestamp", "opendap"),
        "fieldsesgfproject": ("mip_era", "project", "institution_id", "source_id", "experiment_id", "table_id",
                              "variable_id", "grid_label", "frequency", "realm", "product", "creation_date",
                              "variant_label", "further_info_url", "activity_id", "pid"),
        "fieldsunlist": ("checksum", "checksum_type", "tracking_id", "variable_id","mip_era", "project",
                         "institution_id", "source_id", "experiment_id", "table_id", "variable_id", "grid_label",
                         "frequency", "realm", "product", "creation_date", "variant_label", "further_info_url",
                         "activity_id", "pid"),
        "eva": {
            "ensemble": ("mip_era", "activity_id", "institution_id", "source_id", "experiment_id",
                         "table_id", "realm", "grid_label", "version"),
        },
        "fieldsensemble": ("mip_era", "activity_id", "institution_id", "source_id", "experiment_id", "table_id", "grid_label", "realm"),
        "aggregationstable": "cmip6_aggregations",
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

def check_missing_fields(document, fields):
    nulls = ("timestamp")
    for f in fields:
        if f not in document:
            if f in nulls:
                document[f] = None
            else:
                document[f] = ""

def process_url(url):
    protocols = {}
    for u in url:
        if u.endswith("HTTPServer"):
            protocols["HTTPServer"] = u.replace("|application/netcdf|HTTPServer", "")
        elif u.endswith("OPENDAP"):
            protocols["OPENDAP"] = u.replace(".html|application/opendap-html|OPENDAP", "")

    # check that protocols are present
    if "HTTPServer" not in protocols:
        protocols["HTTPServer"] = None
    if "OPENDAP" not in protocols:
        protocols["OPENDAP"] = None

    return protocols

def unlist(document, fields):
    for f in fields:
        if isinstance(document[f], list):
            document[f] = document[f][0]

def dump(project, f, conn):
    cur = conn.cursor()

    for line in open(f):
        j = json.loads(line)

        check_missing_fields(j, config[project]['fieldscore']+config[project]['fieldsesgfproject'])
        unlist(j, config[project]['fieldsunlist'])

        urls = process_url(j["url"])
        j['opendap'] = urls["OPENDAP"]
    
        # insert the file
        tablecols = config[project]["fieldscore"] + config[project]["fieldsesgfproject"]
        cols = ",".join(tablecols)
        values_string = ",".join(["%s"]*len(tablecols))
        values = [j[x] for x in tablecols]
        query = "INSERT INTO {} ({}) VALUES ({}) RETURNING _id;".format(project, cols, values_string)

        cur.execute(query, tuple(values))
        last_file = cur.fetchone()[0]

        # insert aggregation
        cur.execute("SELECT id,name FROM evaprojects")
        for row in cur.fetchall():
            evaid = row[0]
            evaproject = row[1]
            evafields = config[project]["eva"][evaproject]
            aggregation = '_'.join([j[field] for field in evafields])
            tablename = config[project]["aggregationstable"]
            cur.execute("INSERT INTO {} (file,project,name) VALUES (%s,%s,%s)".format(tablename), (last_file, evaid, aggregation))

    cur.close()

if __name__ == "__main__":
    project = "cmip6"
    f = "cmip6.json"

    conn = psycopg2.connect("dbname=eva user=ecimadevilla password=coches1")
    dump(project, f, conn)
    conn.commit()
    conn.close()
