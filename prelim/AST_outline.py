import re
import os
import timeit
import logging
import cx_Oracle
import keyring
import pandas as pd
import geopandas as gpd
import sys
import traceback
from shapely import wkt, wkb
from getpass import getpass
from pathlib import Path
from contextlib import contextmanager

# Use main scripts dir for the project path
current_script_path = Path(__file__).resolve().parents[1]
sys.path.append(str(current_script_path))

from modules.overlap_tool import UniversalOverlapTool as uot
from modules.spreadsheet_to_json import create_spreadsheet_json
from modules.spreadsheet_to_json import clean_dataframe


from config import HOSTNAME, XLSX_DIR

class ASTProcessor:
    def __init__(self, feature, crown_file_num, disp_num, parcel_num, output_dir, connection=None, logger=None):
        """
        Initialize the ASTProcessor.

        Args:

        """
        self.feature = feature
        self.crown_file_number = crown_file_num
        self.disposition_number = disp_num
        self.parcel_number = parcel_num
        self.output_directory = output_dir
        self.connection = connection   ##pass in connection pool resource for batch. For now, use connection method below
        self.logger = logger  ##accept logger from caller. For now, use logger method below.

        # lazy properties
        self.region = None

    def main(self):
        """
        Execute the AST processing workflow.

        Args:
        """

        self.create_output_dir()
        conn = self.connect_to_DB() ##TODO: replace with updated module when complete; handle user inputs, updating keyring, etc.
        # aoi = self.acquire_aoi_spatial()
        self.get_aoi_region()
        json_data = self.get_regional_spreadsheets()
        # self.acquire_tab1_dataframe()
        # self.acquire_tab2_dataframe()
        # self.acquire_tab3_dataframe(aoi, json_data)
        # self.generate_html_maps()
        # self.generate_output_spreadsheets()
        # self.cleanup()


    def create_output_dir(self):
        pass
        ##Check if output dir writable or create output dir

    def connect_to_DB(self): 
        """ Returns a connection to Oracle database"""

        try:
            dbkey = "BCGW"
            username = os.getlogin()
            if not keyring.get_credential(dbkey, username):
                password = getpass("password:")
                
            else:
                password = keyring.get_password(dbkey, username)

            connection = cx_Oracle.connect(user=username, password=password, dsn=HOSTNAME, encoding="UTF-8")
            print  ("....Successffuly connected to the database")
        except Exception as e:
            print(traceback.print_exc())
            raise Exception(f'....Connection failed! Please check your login parameters - {e}')

        return connection

    def acquire_aoi_spatial(self):
        uot.esri_to_gdf() ##This tool specific to esri aoi input but would need to include processing for geoJSON, sqlite, etc.
        ##get aoi as geopandas dataframe, either from file or feature class on disk or from tantalis
        ##from crown_file_number, disposition_number, parcel_number combinations

    def get_aoi_region(self):
        self.region = 'cariboo'
        ##from AOI spatial, get the region or regions, if multiple regional overlaps

    def get_regional_spreadsheets(self):
        """
        Acquires and processes regional and common spreadsheets for report generation.

        Parameters:
            input_xlsx (str): Path to the folder containing input spreadsheets.

        Returns:
            pd.DataFrame: A cleaned and merged DataFrame with processed data.
        """
        #input spreadsheet - MOVE TO CONFIG file
        xlxs_dir = XLSX_DIR

        # Construct file paths - MOVE TO CONFIG file
        common_xls = os.path.join(xlxs_dir, 'one_status_common_datasets.xlsx')
        region_xls = os.path.join(xlxs_dir, f'one_status_{self.region.lower()}_specific.xlsx')
        
        # Load and merge data
        df_common = pd.read_excel(common_xls).iloc[1:]
        df_region = pd.read_excel(region_xls).iloc[1:]
        df_combined = pd.concat([df_common, df_region], ignore_index=True)
        
        # Clean the combined DataFrame
        df_cleaned = clean_dataframe(df_combined)

        # Debug output (optional)
        print(df_cleaned.iloc[:, :5].head(10))

        json_spreadsheet = create_spreadsheet_json(df_cleaned)
        
        return json_spreadsheet

    def acquire_tab1_dataframe(self, aoi, spreadsheet):
        pass
        #summary table of aoi (mapsheet, FN, arch, mines, forests, water, etc.)
        #Leverage query process from UniversalOverlapTool()
        #current code in inactive_dispositions.py & tantalis_bigQuery.py
        spatial = True
        spatial_summary = True
        overlap_tool = uot(aoi, spreadsheet, spatial, spatial_summary)
        overlap_tool.main()

    def acquire_tab2_dataframe(self, aoi, spreadsheets):
        pass
        #inactives
        #Leverage query process from UniversalOverlapTool()
        #current code in inactive_dispositions.py & tantalis_bigQuery.py
        overlap_tool = uot(aoi, spreadsheets)
        overlap_tool.main()

    def acquire_tab3_dataframe(self, aoi, spreadsheets):
        pass
        ##COMMON MODULE FOR MULTIPLE USES
        # Call to Ovelap tool, passing in aoi sptial and spreadsheets
        # returning dataframe and perhaps a GeoPackage of data
        # NEED PARAMETER TO STATE WHICH METRICS TO INCLUDE; spatial=True, spatial_summary=False, etc.)
        # Some returned dataframes will not require the spatial data or the summary of feature
        overlap_tool = uot(aoi, spreadsheets)
        overlap_tool.main()

    def generate_html_maps(Geopackage):
        # CUSTOM MODULE FOR AST (HTML maps for FCBC)
        #iterate through GeoPackage and produce maps
        #Current code in fc_to_html.py
        html_maps = HtmlMapsGenerator(Geopackage)
        html_maps.main()

    def generate_output_spreadsheets(dataframes):
        # CUSTOM MODULE FOR AST (Tabs 1-3)
        #iterate through geopandas dataframe(s) and build output spreadsheet.
        reports = ASTReportGenerator(dataframes)
        reports.main()

    def cleanup():
        pass
        #cleanup connections, scratch files, intermediate data, temp folders, etc.

if __name__ == '__main__':
    ast = ASTProcessor(feature=None, crown_file_num=None, disp_num=None, parcel_num=None, output_dir=None)
    ast.main()