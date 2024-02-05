import netCDF4
import time
import sys
import numpy as np
import xml.etree.ElementTree as ET

if __name__ == "__main__":
    xml = '''
    <aggregation type="joinNew" dimName="variant_label">
        <variableAgg name="tas"></variableAgg>
        <netcdf coordValue="r10i1p1f2">
            <aggregation type="joinExisting" dimName="time">
                <netcdf location="http://esgf-data02.diasjp.net/thredds/dodsC/esg_dataroot/CMIP6/ScenarioMIP/MIROC/MIROC-ES2L/ssp245/r10i1p1f2/3hr/tas/gn/v20210107/tas_3hr_MIROC-ES2L_ssp245_r10i1p1f2_gn_201501010300-201601010000.nc"/>
            </aggregation>
        </netcdf>
    </aggregation>
    '''
    
    ns = "http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"
    ET.register_namespace("", ns)
    # Parse XML
    tree = ET.parse(sys.argv[1])
    netcdf_elements = tree.findall('.//{http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2}netcdf[@location]')
    
    for netcdf_element in netcdf_elements:
        fname = netcdf_element.attrib['location']
        print(fname)
        with netCDF4.Dataset(fname) as nc:
            times = nc["time"][...].ravel()
            time0 = times[0]

            time_el = ET.Element('variable')
            time_el.attrib["name"] = "time"

            diffs = np.unique(np.diff(times))
            if len(diffs) == 1:
                values_el = ET.Element('values')
                values_el.attrib['increment'] = str(diffs[0])
                values_el.attrib['start'] = str(time0)
            else:
                values_el = ET.Element('values')
                values_el.text = " ".join([str(x) for x in times])

            time_el.append(values_el)
            netcdf_element.append(time_el)
            netcdf_element.attrib['ncoords'] = str(len(times))
        time.sleep(1) # avoid being blocked by some servers eg: esgf.ceda.ac.uk

    ## Generate modified XML
    #modified_xml = ET.tostring(tree, encoding='unicode')

    #et = etree.ElementTree(root)
    #et.write('output.xml', pretty_print=True)
    #
    ## Print modified XML
    #print(modified_xml)
    tree.write('output.xml')
