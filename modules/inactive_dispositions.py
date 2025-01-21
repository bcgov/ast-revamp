import os
import pyodbc
import pandas as pd
import arcpy
import sys
from pathlib import Path
from tantalis_bigQuery import load_sql

# Use main scripts dir for the project path
current_script_path = Path(__file__).resolve().parents[1]
sys.path.append(str(current_script_path))


def connect_to_DB (driver,server,port,dbq, username,password):
    """ Returns a connection to Oracle database"""
    try:
        connectString ="""
                    DRIVER={driver};
                    SERVER={server}:{port};
                    DBQ={dbq};
                    Uid={uid};
                    Pwd={pwd}
                       """.format(driver=driver,server=server, port=port,
                                  dbq=dbq,uid=username,pwd=password)

        connection = pyodbc.connect(connectString)
        print  ("...Successffuly connected to the database")
    except:
        raise Exception('...Connection failed! Please check your connection parameters')

    return connection


def read_query(connection,query):
    "Returns a df containing SQL Query results"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        cols = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame.from_records(rows, columns=cols)
    
    finally:
        if cursor is not None:
            cursor.close()
            connection.close()


def format_parcels_list(parcel_list):
    """Split the parcels list into chunks of 1000 items.
       (Oracle SQL doesn't support IN clauses with more than 1000 entry)"""
    
    n = 1000
    array = [parcel_list[i:i + n] for i in range(0, len(parcel_list), n)]

    #Construct SQL string
    first_str = "("
    middle_str  = ''
    last_str = ")"

    for i, value in enumerate (array):
        joined = '(' + ','.join(str(x) for x in value) + ')'
        add_string = 'IP.INTRID_SID IN ' + str(joined)

        if i < len(array)-1:
            add_string = add_string + ' OR '
        else:
            pass
        
        middle_str += add_string

    parcels_q_str = first_str + middle_str +  last_str

    return parcels_q_str 


def get_inact_info(df_inact_lands):
    """Harmonizes column names of inactive dfs as per ILRR schema and returns values Lists.
       Only Inactive Lands df is provided for now. Add others as required."""  
    
    df_inact_lands['HOLDER_NAME'] = df_inact_lands['HOLDER_ORGANNSATION_NAME'].fillna('')\
                                    + df_inact_lands['HOLDER_INDIVIDUAL_NAME'].fillna('')

    # delete duplicates: same dispID and same Holder name.
    df_inact_lands.drop_duplicates(subset = ['DISPOSITION_TRANSACTION_SID', 'HOLDER_NAME'], inplace=True)
    df_inact_lands.drop(['HOLDER_ORGANNSATION_NAME', 'HOLDER_INDIVIDUAL_NAME'], inplace=True, axis=1)
    
    # Merge same dispID with multiple Holders into the same row
    gr_cols = [col for col in df_inact_lands.columns]
    gr_cols.remove('HOLDER_NAME')
    df_inact_lands = df_inact_lands.groupby(gr_cols)['HOLDER_NAME'].apply(' / '.join).reset_index()
    
    # transform to ILLR col names and schema
    df_inact_lands['interest_status'] = 'INACTIVE'
    df_inact_lands['interest_type'] = df_inact_lands['PURPOSE_NME'] + ' ' + df_inact_lands['TYPE_NME']
    df_inact_lands['dpr_registry_name'] = 'CROWN LANDS'
    df_inact_lands['business_identifier'] = 'Disp Trans SID: ' + df_inact_lands['DISPOSITION_TRANSACTION_SID'].astype(int).astype(str)\
                                    + ' ' + 'FILE NUMBER: ' +  df_inact_lands['FILE_CHR']
    df_inact_lands['responsible_agency'] = 'FLNR'
    df_inact_lands['summary_holders_ilrr_identifier'] = df_inact_lands['HOLDER_NAME']
    
    #Create lists of values
    ilrr_info = {}
    ilrr_info['interest_status'] = df_inact_lands['interest_status'].tolist()
    ilrr_info['interest_type']  = df_inact_lands['interest_type'].tolist()
    ilrr_info['dpr_registry_name']  = df_inact_lands['dpr_registry_name'].tolist()
    ilrr_info['business_identifier']  = df_inact_lands['business_identifier'].tolist()
    ilrr_info['responsible_agency']  = df_inact_lands['responsible_agency'].tolist()
    ilrr_info['summary_holders_ilrr_identifier']  = df_inact_lands['summary_holders_ilrr_identifier'].tolist()
    
    return ilrr_info


def get_oracle_driver():
    """Captures the latest Oracle Driver installed on the GTS Server"""
    oracle_drivers = [driver for driver in pyodbc.drivers() if 'Oracle' in driver]
    if oracle_drivers:
        latest_driver = max(oracle_drivers)
        return latest_driver
    else:
        arcpy.AddWarning("TAB2 could not be generated. Oracle drivers do not exist on the GTS. Please contact geospatialservices.waterland@gov.bc.ca for support")
        return


def execute_process(parcel_list,bcgw_user,bcgw_pwd,oracle_driv):
    """Generates a csv of inactive Lands dispositions"""
    
    print('Connecting to BCGW.')
    driver = oracle_driv #'Oracle in OraClient12Home2'
    server = config.CONNSERVER
    port = config.CONNPORT
    dbq = config.CONNDBQ
    hostname = config.CONNINSTANCE

    connection = connect_to_DB(driver,server,port,dbq,bcgw_user,bcgw_pwd)
    
    print ('Loading SQL queries.')
    sql = load_sql()
    
    print ('Execute the query.')
    parcels_q_str = format_parcels_list(parcel_list)

    query = sql['inactive_lands'].format(prcl=parcels_q_str)# add the parcels list to the SQL query

    df_inact_lands = read_query(connection,query) #execute the query and store results in a dataframe

    print ('Retrieve Inactive info.')
    ilrr_info = get_inact_info(df_inact_lands)

    return ilrr_info


if __name__=="__main__":

    bcgw_user = ''
    bcgw_pwd = ''

    aoi = ''
    
    print ('Retrieving the parcels list')
    sde = r"h:\arcpro\bcgw.sde"
    parcel_fc = os.path.join(sde, r'WHSE_TANTALIS.TA_INTEREST_PARCEL_SHAPES')
    clip_parcel = arcpy.Clip_analysis(parcel_fc, aoi, r"memory\parcel_clip")
    result = int(arcpy.GetCount_management(clip_parcel).getOutput(0))
    print('{} has {} records'.format("Tantalis Parcels", result))
    if result > 0:
        parcel_list = [row[0] for row in arcpy.da.SearchCursor(clip_parcel,['INTRID_SID'])]
        print(len(parcel_list))
        drvr = get_oracle_driver()
        ilrr = execute_process(parcel_list,bcgw_user,bcgw_pwd,drvr)
        print(ilrr)

    else:
        print("No interest parcels returned!")