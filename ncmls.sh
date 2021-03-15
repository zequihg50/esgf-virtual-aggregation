#!/bin/bash

set -u

trap exit SIGINT SIGKILL

# defaults
columns=OPENDAP,index_node,data_node,size,replica,version,retracted,_timestamp,_version_,checksum,checksum_type,_eva_ensemble_aggregation,_eva_variable_aggregation,_eva_no_frequency
overwrite=0
ncmls=content/public/EVA
pickles=pickles
publisher=publisher
sources=""
version="$(date +%Y%m%d)"

usage() {
    echo "${0}"
}

while [[ $# -gt 0 ]]
do
    case "$1" in
    --columns)
        columns="$2"
        shift 2
        ;;
    -h | --help)
        usage >&2
        exit 1
        ;;
    --ncmls)
        ncmls="$2"
        shift 2
        ;;
    --overwrite)
        overwrite="1"
        shift 1
        ;;
    --pickles)
        pickles="$2"
        shift 2
        ;;
    --publisher)
        publisher="$2"
        shift 2
        ;;
    --version)
        version="$2"
        shift 2
        ;;
    -*)
        echo 'Unknown option, use -h for help, exiting...'
        exit 1
        ;;
    *)
        sources="${sources} $1"
        shift 1
        ;;
    esac
done

if [ -z ${version} ] ; then
    echo 'Please, indicate the version of the EVA run using --version, exiting...' >&2
    exit 1
fi

mkdir -p ${pickles} ${ncmls}

# log to avoid generating already existing ncmls
logp=logp
if [ ! -f ${logp} ] ; then
    touch ${logp}
fi

if [ -z "${sources}" ] ; then
    cat <&0
else
    cat ${sources}
fi | while read csv
do
    basename=${csv##*/}
    pickle=${pickles}/${basename}

    # If pickle is processed and overwrite set to false, ignore
    if grep -F -q "${basename}" ${logp} && [ "${overwrite}" -eq 0 ] ; then
        echo "Ignoring ${basename}..." >&2
    elif [ ! -s "${csv}" ] ; then
        echo "Size0 ${csv}..." >&2
    else
        python -W ignore ${publisher}/todf.py -f ${csv} --numeric size -v time --col 0 --cols ${columns} ${pickle}

        # log
        echo ${pickle} >&2
        if ! grep -F -q "${basename}" ${logp} ; then
            echo "${basename}" >> ${logp}
        fi
    fi
done | while read pickle
do
    python cmip6.py ${pickle}
done | while read pickle
do
    creation="$(date -u +%FT%T)Z"
    ncml="${ncmls}/ensemble/{mip_era}/{institution_id}/{mip_era}_{activity_id}_{institution_id}_{source_id}_{experiment_id}_{table_id}_{frequency}_{realm}_{grid_label}_${version}_{data_node}_v{version}.ncml"
    python ${publisher}/jdataset.py -d ${ncml} -o variable_col=variable_id -o eva_version="${version}" -o creation="${creation}" -t templates/cmip6.ncml.j2 ${pickle}
    #rm -f "${pickle}"
done
