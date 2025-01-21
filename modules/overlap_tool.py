import re
import os
import timeit
import logging
import oracledb
import keyring
import pandas as pd
import geopandas as gpd
import json
import re
import sys
from osgeo import ogr
from shapely import wkt, wkb
from getpass import getpass
from pathlib import Path
from contextlib import contextmanager

# Use main scripts dir for the project path
current_script_path = Path(__file__).resolve().parents[1]
sys.path.append(str(current_script_path))


class GeoDataProcessor:
    def __init__(self, input_json):
        """
        Initialize with the input JSON data.

        Args:
            input_json (dict): Input JSON data.
        """
        self.data = input_json

    def process_entry(self, entry):
        """Process a single entry and generate the spatial query summary."""
        table_summary = entry.get("table_summary", {})
        
        # Extract fields
        category = table_summary.get("category")
        feature_name = table_summary.get("feature_name")
        table = table_summary.get("table")
        query = table_summary.get("query")
        buffer_distance = table_summary.get("buffer", 0)
        label_field = table_summary.get("label_field")
        summary_fields = table_summary.get("summary_fields", [])

        # Format feature_class
        formatted_feature_class = re.sub(r"\s+", "_", feature_name.strip())

        # Determine the data type using GDAL/OGR
        data_type = self.determine_data_type(table)

        # Ensure label_field is added only if not in summary_fields
        if label_field not in summary_fields:
            summary_fields.insert(0, label_field)

        # Generate schema
        schema = summary_fields

        # Generate spatial query summary
        spatial_query_summary = {
            "data_type": data_type,
            "feature_class": formatted_feature_class,
            "source_table": table,
            "srid": self.extract_srid(table),
            "overlay_intersection_query": self.generate_overlay_query(query, buffer_distance),
            "schema": schema
        }

        # Combine spatial query summary with the original entry
        entry["spatial_query_summary"] = spatial_query_summary
        return entry

    def process_all_entries(self):
        """Process all entries in the JSON file."""
        if isinstance(self.data, list):
            return [self.process_entry(entry) for entry in self.data]
        elif isinstance(self.data, dict):
            return self.process_entry(self.data)
        else:
            raise ValueError("Unsupported JSON structure. Must be a list or dictionary.")

    def determine_data_type(self, table_name):
        """
        Determine the data type using GDAL/OGR.

        Args:
            table_name (str): Name or path of the table.

        Returns:
            str: Data type (e.g., "shapefile", "file geodatabase", "oracle").
        """
        driver_name = None
        try:
            datasource = ogr.Open(table_name)
            if datasource:
                driver_name = datasource.GetDriver().GetName()
                datasource = None
        except Exception as e:
            driver_name = "unknown"
        return driver_name or "unknown"

    def extract_srid(self, table_name):
        """
        Extract the spatial reference ID (SRID) using GDAL/OGR.

        Args:
            table_name (str): Name or path of the table.

        Returns:
            int: SRID value.
        """
        try:
            datasource = ogr.Open(table_name)
            if datasource:
                layer = datasource.GetLayer()
                spatial_ref = layer.GetSpatialRef()
                datasource = None
                return int(spatial_ref.GetAuthorityCode(None)) if spatial_ref else 3005
        except Exception as e:
            return 3005  # Default to 4326 if extraction fails

    def generate_overlay_query(self, query, buffer_distance):
        """
        Generate an overlay intersection query.

        Args:
            query (str): Original query string.
            buffer_distance (float): Buffer distance.

        Returns:
            str: Overlay intersection query.
        """
        if buffer_distance > 0:
            overlay_query = f"ST_Intersects(geometry, ST_Buffer(AOI, {buffer_distance}))"
        else:
            overlay_query = "ST_Intersects(geometry, AOI)"
        
        if query:
            overlay_query += f" AND ({query})"
        
        return overlay_query



class UniversalOverlapTool:
    def __init__(self, aoi, spreadsheet, connection=None, logger=None):
        """
        Initialize the UniversalOverlapTool.

        Args:

        """
        self.aoi = aoi
        self.spreadsheet = spreadsheet
        
        self.connection = connection   ##accept connection for now
        self.logger = logger  ##accept logger from caller.

    def main(self):
        pass


    def read_query(self, connection, query, bvars):
        "Returns a df containing SQL Query results"
        cursor = connection.cursor()
        cursor.execute(query, bvars)
        names = [x[0] for x in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=names)
        
        return df    
    
            
    @staticmethod
    def esri_to_gdf(fc_path):
        """Returns a Geopandas file (gdf) based on 
        an ESRI format vector (shp or featureclass/gdb)"""
        
        if '.shp' in fc_path: 
            gdf = gpd.read_file(fc_path)
        
        elif '.gdb' in fc_path:
            l = fc_path.split ('.gdb')
            gdb = l[0] + '.gdb'
            fc = os.path.basename(fc_path)
            gdf = gpd.read_file(filename= gdb, layer= fc)
            
        else:
            raise Exception ('Format not recognized. Please provide a shp or featureclass (gdb)!')
        
        return gdf



    def df_2_gdf (df, crs):
        """ Return a geopandas gdf based on a df with Geometry column"""
        df['SHAPE'] = df['SHAPE'].astype(str)
        df['geometry'] = gpd.GeoSeries.from_wkt(df['SHAPE'])
        gdf = gpd.GeoDataFrame(df, geometry='geometry')
        #df['geometry'] = df['SHAPE'].apply(wkt.loads)
        #gdf = gpd.GeoDataFrame(df, geometry = df['geometry'])
        gdf.crs = "EPSG:" + str(crs)
        del df['SHAPE']
        
        return gdf



    def multipart_to_singlepart(gdf):
        """Converts a multipart gdf to singlepart gdf """
        gdf['dissolvefield'] = 1
        gdf = gdf.dissolve(by='dissolvefield')
        gdf.reset_index(inplace=True)
        gdf = gdf[['geometry']] #remove all columns
            
        return gdf



    def get_wkb_srid (gdf):
        """Returns SRID and WKB objects from gdf"""
        print(f"gdf: {type(gdf)}")
        srid = gdf.crs.to_epsg()
        
        geom = gdf['geometry']
        print(f"geom: {type(geom)}")

        wkb_aoi = geom.to_wkb().iloc[0]

        geom = gdf['geometry'].iloc[0]
        
        # if geometry has Z values, flatten geometry
        if geom.has_z:
            wkb_aoi = wkb.dumps(geom, output_dimension=2)
            
        
        return wkb_aoi, srid
        

    def get_table_cols (item_index,df_stat):
        """Returns table and field names from the AST datasets spreadsheet"""
        #df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
        df_stat_item = df_stat.loc[[item_index]]
        df_stat_item.fillna(value='nan',inplace=True)

        table = df_stat_item['Datasource'].iloc[0].strip()
        
        fields = []
        fields.append(str(df_stat_item['Fields_to_Summarize'].iloc[0].strip()))

        for f in range (2,7):
            for i in df_stat_item['Fields_to_Summarize' + str(f)].tolist():
                if i != 'nan':
                    fields.append(str(i.strip()))

        col_lbl = df_stat_item['map_label_field'].iloc[0].strip()
        
        if col_lbl != 'nan' and col_lbl not in fields:
            fields.append(col_lbl)
        
        if table.startswith('WHSE') or table.startswith('REG'):       
            cols = ','.join('b.' + x for x in fields)

            # TEMPORARY FIX:  for empty column names in the COMMON AST input spreadsheet
            if cols == 'b.nan':
                cols = 'b.OBJECTID'
        else:
            cols = fields
            # TEMPORARY FIX:  for empty column names in the REGION AST input spreadsheet
            if cols[0] == 'nan':
                cols = []
        
        return table, cols, col_lbl

            

    def get_def_query (item_index,df_stat):
        """Returns an ORacle SQL formatted def query (if any) from the AST datasets spreadsheet"""
        #df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
        df_stat_item = df_stat.loc[[item_index]]
        df_stat_item.fillna(value='nan',inplace=True)

        def_query = df_stat_item['Definition_Query'].iloc[0].strip()

        def_query = def_query.strip()
        
        if def_query == 'nan':
            def_query = " "
            
        else:
            def_query = def_query.replace('"', '')
            def_query = re.sub(r'(\bAND\b)', r'\1 b.', def_query)
            def_query = re.sub(r'(\bOR\b)', r'\1 b.', def_query)
            
            if def_query[0] == "(":
                def_query = def_query.replace ("(", "(b.") 
                def_query = "(" + def_query + ")"
            else:
                def_query = "b." + def_query
            
            def_query = 'AND (' + def_query + ')'
        
        
        return def_query



    def get_radius (item_index, df_stat):
        """Returns the buffer distance (if any) from the AST common datasets spreadsheet"""
        #df_stat = df_stat.loc[df_stat['Featureclass_Name(valid characters only)'] == item]
        df_stat_item = df_stat.loc[[item_index]]
        df_stat_item.fillna(value=0,inplace=True)
        df_stat_item['Buffer_Distance'] = df_stat_item['Buffer_Distance'].astype(int)
        radius = df_stat_item['Buffer_Distance'].iloc[0]
        
        return radius


    def load_queries():
        sql = {}

        sql ['aoi'] = """
                        SELECT SDO_UTIL.TO_WKTGEOMETRY(a.SHAPE) SHAPE
                        
                        FROM  WHSE_TANTALIS.TA_CROWN_TENURES_SVW a
                        
                        WHERE a.CROWN_LANDS_FILE = :file_nbr
                            AND a.DISPOSITION_TRANSACTION_SID = :disp_id
                            AND a.INTRID_SID = :parcel_id
                    """
                            
        sql ['geomCol'] = """
                        SELECT column_name GEOM_NAME
                        
                        FROM  ALL_SDO_GEOM_METADATA
                        
                        WHERE owner = :owner
                            AND table_name = :tab_name
                            
            
                        """    
                        
        sql ['srid'] = """
                        SELECT s.{geom_col}.sdo_srid SP_REF
                        FROM {tab} s
                        WHERE rownum = 1
                    """
                            
        sql ['overlay'] = """
                        SELECT {cols},
                        
                            CASE WHEN SDO_GEOM.SDO_DISTANCE(b.{geom_col}, a.SHAPE, 0.5) = 0 
                                THEN 'INTERSECT' 
                                ELSE 'Within ' || TO_CHAR({radius}) || ' m'
                                END AS RESULT,
                                
                            SDO_UTIL.TO_WKTGEOMETRY(b.{geom_col}) SHAPE
                        
                        FROM WHSE_TANTALIS.TA_CROWN_TENURES_SVW a, {tab} b
                        
                        WHERE a.CROWN_LANDS_FILE = :file_nbr
                            AND a.DISPOSITION_TRANSACTION_SID = :disp_id
                            AND a.INTRID_SID = :parcel_id
                            
                            AND SDO_WITHIN_DISTANCE (b.{geom_col}, a.SHAPE,'distance = {radius}') = 'TRUE'
                            
                            {def_query}  
                        """ 
                                        
        sql ['overlay_wkb'] = """
                        SELECT {cols},
                        
                            CASE WHEN SDO_GEOM.SDO_DISTANCE(b.{geom_col}, SDO_GEOMETRY(:wkb_aoi, :srid_t), 0.5) = 0 
                                THEN 'INTERSECT' 
                                ELSE 'Within ' || TO_CHAR({radius}) || ' m'
                                END AS RESULT,
                                
                            SDO_UTIL.TO_WKTGEOMETRY(b.{geom_col}) SHAPE
                        
                        FROM {tab} b
                        
                        WHERE SDO_WITHIN_DISTANCE (b.{geom_col}, 
                                                SDO_GEOMETRY(:wkb_aoi, :srid),'distance = {radius}') = 'TRUE'
                            {def_query}   
                        """ 
        return sql



    def get_geom_colname (connection,cursor,table,geomQuery):
        """ Returns the geometry column of BCGW table name: can be either SHAPE or GEOMETRY"""
        el_list = table.split('.')

        bvars_geom = {'owner':el_list[0].strip(),
                    'tab_name':el_list[1].strip()}
        df_g = self.read_query(connection,cursor,geomQuery, bvars_geom)
        
        geom_col = df_g['GEOM_NAME'].iloc[0]

        return geom_col



    def get_geom_srid (connection,cursor,table,geom_col,sridQuery):
        """ Returns the SRID of the BCGW table"""

        sridQuery = sridQuery.format(tab=table,geom_col=geom_col)
        df_s = self.read_query(connection,cursor,sridQuery,{})
        
        srid_t = df_s['SP_REF'].iloc[0]

        return srid_t