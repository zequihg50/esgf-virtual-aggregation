#!/bin/bash

trap exit SIGINT SIGKILL

set -e

content=$(pwd)/content
root_ncmls=${content}/public/EVA
root_catalogs=${content}/EVA
root_catalog=${catalogs}/catalog.xml

#{% set attrs = ['activity_id', 'Conventions', 'data_specs_version', 'experiment', 'experiment_id', 'forcing_index',
#                'frequency', 'grid', 'grid_label', 'initialization_index', 'institution', 'institution_id', 'license',
#                'mip_era', 'nominal_resolution', 'physics_index', 'product', 'realization_index', 'realm',
#                'source', 'source_id', 'source_type', 'sub_experiment', 'sub_experiment_id', 'table_id',
#                'variable_id', 'variant_label', 'cmor_version'] %}
#{% set no_parent_attrs = ['branch_method', 'parent_activity_id', 'parent_experiment_id', 'parent_mip_era',
#                          'parent_source_id', 'parent_time_units', 'parent_variant_label'] %}
#{% set omit_attrs = ['branch_time_in_child', 'branch_time_in_parent'] %}
# $1 ncml
cmip6_dataset() {
    prefix=EVA/ensemble/CMIP6

    echo '  <dataset name="'${1}'" ID="'${prefix}/${1}'">'
    while read ncml
    do
        name=${ncml##*/}
        name=${name%.ncml}
        urlPath=${prefix}/${name}
        id=${urlPath}
        size=$(awk '/<attribute name="size"/{gsub("[^0-9]", ""); print; exit}' ${ncml})
        modified=$(stat --format=%y ${ncml})
        location=content/EVA/${ncml##*public/EVA/}

        # ncml attributes
        replica=$(awk '/<attribute name="replica"/{sub(".*value=\"", ""); sub("\".*", ""); print; exit}' ${ncml})

        echo '    <dataset name="'${name}'"'
        echo '        ID="'${id}'"'
        echo '        urlPath="'${urlPath}'">'
        echo '      <metadata inherited="true">'
        echo '        <serviceName>virtual</serviceName>'
        echo '        <dataSize units="bytes">'"${size}"'</dataSize>'
        echo '        <date type="modified">'"${modified}"'</date>'
        echo '      </metadata>'
        echo '      <property name="replica" value="'${replica}'"/>'
        echo '      <netcdf xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"'
        echo '              location="'${location}'" />'
        echo '    </dataset>'
    done
    echo '  </dataset>'
    echo ''
}

# $1 catalog
ref() {
    href=${1##*/}
    title=${href%.xml}
    size=$(awk '/<dataSize/{gsub("[^0-9]", ""); total+=$0}END{print total}' ${1})
    modified=$(stat --format=%y ${1})

    echo '  <catalogRef xlink:title="'"${title}"'" xlink:href="'${href}'" name="">'
    echo '    <dataSize units="bytes">'"${size}"'</dataSize>'
    echo '    <date type="modified">'"${modified}"'</date>'
    echo '  </catalogRef>'
    echo ''
}

# $1 name
init_catalog() {
    echo '<?xml version="1.0" encoding="UTF-8"?>'
    echo "<catalog name=\"$1\""
    echo '         xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"'
    echo '         xmlns:xlink="http://www.w3.org/1999/xlink">'
    echo ''
    echo '  <service name="all" serviceType="Compound" base="">'
    echo '    <service base="/thredds/fileServer/" name="http" serviceType="FileServer" />'
    echo '    <service base="/thredds/dodsC/" name="odap" serviceType="OpenDAP"/>'
    echo '    <service base="/thredds/dap4/" name="dap4" serviceType="DAP4" />'
    echo '    <service base="/thredds/wcs/" name="wcs" serviceType="WCS" />'
    echo '    <service base="/thredds/wms/" name="wms" serviceType="WMS" />'
    echo '    <service base="/thredds/ncss/grid/" name="ncssGrid" serviceType="NetcdfSubset" />'
    echo '    <service base="/thredds/ncss/point/" name="ncssPoint" serviceType="NetcdfSubset" />'
    echo '    <service base="/thredds/cdmremote/" name="cdmremote" serviceType="CdmRemote" />'
    echo '    <service base="/thredds/cdmrfeature/grid/" name="cdmrFeature" serviceType="CdmrFeature" />'
    echo '    <service base="/thredds/iso/" name="iso" serviceType="ISO" />'
    echo '    <service base="/thredds/ncml/" name="ncml" serviceType="NCML" />'
    echo '    <service base="/thredds/uddc/" name="uddc" serviceType="UDDC" />'
    echo '  </service>'
    echo ''
    echo '  <service name="virtual" serviceType="Compound" base="">'
    echo '    <service base="/thredds/dodsC/" name="odap" serviceType="OpenDAP"/>'
    echo '    <service base="/thredds/dap4/" name="dap4" serviceType="DAP4" />'
    echo '    <service base="/thredds/wcs/" name="wcs" serviceType="WCS" />'
    echo '    <service base="/thredds/wms/" name="wms" serviceType="WMS" />'
    echo '    <service base="/thredds/ncss/grid/" name="ncssGrid" serviceType="NetcdfSubset" />'
    echo '    <service base="/thredds/ncss/point/" name="ncssPoint" serviceType="NetcdfSubset" />'
    echo '    <service base="/thredds/cdmremote/" name="cdmremote" serviceType="CdmRemote" />'
    echo '    <service base="/thredds/cdmrfeature/grid/" name="cdmrFeature" serviceType="CdmrFeature" />'
    echo '    <service base="/thredds/iso/" name="iso" serviceType="ISO" />'
    echo '    <service base="/thredds/ncml/" name="ncml" serviceType="NCML" />'
    echo '    <service base="/thredds/uddc/" name="uddc" serviceType="UDDC" />'
    echo '  </service>'
    echo ''
}

# EVA Ensemble CMIP6
ncmls=${root_ncmls}/ensemble/CMIP6
project_catalog=${root_catalogs}/ensemble/CMIP6/catalog.xml
mkdir -p ${root_catalogs}/ensemble/CMIP6

init_catalog "EVAEnsemble_CMIP6" >${project_catalog}
find ${ncmls} -mindepth 1 -maxdepth 1 -type d -printf '%f\n' | while read institute
do
    catalog=${root_catalogs}/ensemble/CMIP6/${institute}.xml
    init_catalog "EVAEnsemble_CMIP6_${institute}" >${catalog}

    # group by "masterid" (a master id contains all EVA versions, all data nodes and all ESGF versions of a dataset)
    find ${ncmls}/${institute} -type f -name '*.ncml' -printf '%f\n' | cut -d_ -f1-9 | sort -uV | while read masterid
    do
        find ${ncmls}/${institute} -type f -name '*.ncml' | grep ${masterid} | sort -V | cmip6_dataset ${masterid} >>${catalog}
    done
    echo '</catalog>' >>${catalog}

    # reference from parent catalog
    ref ${catalog} >>${project_catalog}

    # print created catalog
    echo ${catalog}
done
echo '</catalog>' >>${project_catalog}
echo ${project_catalog}
