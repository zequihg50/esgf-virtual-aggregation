#!/bin/bash

content=tds-content
ncmls=${content}/public/EVA/variable-aggregation
catalogs=${content}/devel/EVA/variable-aggregation
cmip6=${catalogs}/cmip6.xml

. publisher/contrib/esgf/catalog.sh

init_catalog EVA-CMIP6 > ${cmip6}
find ${ncmls} -type f | sort -V | while read ncml
do
  name=${ncml##*/}
  name=${name%.ncml}
  urlPath=devel/EVA/variable-aggregation/${name}
  id=${urlPath}
  size=0
  last_modified=$(stat --format=%y ${ncml})
  location=content/${ncml##*public/}
  
  dataset1 "${name}" "${id}" "${urlPath}" "${size}" "${last_modified}" "${location}" >> ${cmip6}
done
echo '</catalog>' >> ${cmip6}
echo ${cmip6}
