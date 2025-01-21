'''
This script generates interavtive HTML maps.

Update: May 14, 2024.
'''
import warnings
warnings.simplefilter(action='ignore')

import os
import timeit
import base64
import numpy as np
import pandas as pd
import geopandas as gpd
import fiona
import shapely.wkt as wkt
import folium
from folium.plugins import MeasureControl, MousePosition,FloatImage, MiniMap, Search, GroupedLayerControl
from branca.element import Template, MacroElement

import mapstyle


class HTMLGenerator:
    def __init__(self, common_xls, region_xls, status_gdb, out_location):
        self.status_gdb = status_gdb
        self.out_loc = out_location
        self.common_xls = common_xls
        self.region_xls = region_xls


    def get_input_xlsx(self):
        """returns a dataframe of status input xlsxs """
        df_stat_c = pd.read_excel(self.common_xls)
        df_stat_r = pd.read_excel(self.region_xls)
        
        df_stat = pd.concat([df_stat_c, df_stat_r])
        df_stat.dropna(how='all', inplace=True)
    
        df_stat['Category'].fillna(method='ffill', inplace=True)
        
        df_stat = df_stat.reset_index(drop=True)
        
        return df_stat


    def create_map_template(self, title='Placeholder for title',Xcenter=0,Ycenter=0):
        """Returns an empty folium map object"""
        # Create a map object
        map_obj = folium.Map()
        
        # Add GeoBC basemap to the map
        wms_url = 'https://maps.gov.bc.ca/arcgis/rest/services/province/web_mercator_cache/MapServer/tile/{z}/{y}/{x}'
        wms_attribution = 'GeoBC, DataBC, TomTom, © OpenStreetMap Contributors'
        folium.TileLayer(
            tiles=wms_url,
            name='GeoBC Basemap',
            attr=wms_attribution,
            overlay=False,
            control=True,
            transparent=True).add_to(map_obj)
        
        # Add a satellite basemap to the map
        satellite_url = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'
        satellite_attribution = 'Tiles &copy; Esri'
        folium.TileLayer(
            tiles=satellite_url,
            name='Imagery Basemap',
            attr=satellite_attribution,
            overlay=False,
            control=True).add_to(map_obj)
        
        # create a title and button to refresh the map to the initial view
        map_var_nme = map_obj.get_name()
        
        title_refresh = """
        <div style="position: fixed; 
            top: 8px; left: 70px; width: 150px; height: 70px; 
            background-color:transparent; border:0px solid grey;z-index: 900;">
            <h5 style="font-weight:bold;color:#DE1610;white-space:nowrap;">{}</h5>
            <button style="font-weight:bold; color:#DE1610" 
                    onclick="{}.setView([{}, {}], 16)">Refresh View</button>
        </div>
    
        """.format(title,map_var_nme,Ycenter,Xcenter)
        
        map_obj.get_root().html.add_child(folium.Element(title_refresh))
        
        # Add measure controls to the map
        map_obj.add_child(MeasureControl(primary_length_unit='meters', 
                                        secondary_length_unit='kilometers',
                                        primary_area_unit='hectares'))
        
        # Add mouse psotion to the map
        MousePosition().add_to(map_obj)
    
        # Add Lat/Long Popup to the map
        map_obj.add_child(folium.features.LatLngPopup())
    
        
        # Add logo to the map
        logo_path = (r"path\to\logo")
        b64_content = base64.b64encode(open(logo_path, 'rb').read()).decode('utf-8')
        float_image = FloatImage('data:image/png;base64,{}'.format(b64_content), bottom=3, left=2)
        float_image.add_to(map_obj)
        
        #Add a Mini Map
        minimap = MiniMap(position="bottomright")
        map_obj.add_child(minimap)
    
        # Add custom css style to the map
        app_css = mapstyle.map_css
        style = MacroElement()
        style._template = Template(app_css)
        map_obj.get_root().add_child(style)
        
        return map_obj


    def generate_html_maps(self):
        """Creates a HTML map for each feature class in gdb"""

        print('\nReading input xlsxs')
        df_st= self.get_input_xlsx()
        
        print ('\nPreparing Layers for mapping')
        # Read the AOI feature class into a gdf 
        gdf_aoi = gpd.read_file(filename=self.status_gdb, layer= 'aoi')
        
        # Create a dict of buffered gdfs
        bf_gdfs= {'aoi_500': gpd.GeoDataFrame(geometry= gdf_aoi.buffer(500), crs= gdf_aoi.crs), 
                  'aoi_1000': gpd.GeoDataFrame(geometry= gdf_aoi.buffer(1000), crs= gdf_aoi.crs), 
                  'aoi_5000': gpd.GeoDataFrame(geometry= gdf_aoi.buffer(5000), crs= gdf_aoi.crs) 
                  }
        
        print ('\nCreating a map template')
        # Create an all-layers map
        centroids = gdf_aoi.to_crs(4326).centroid
        Xcenter = centroids.x[0]
        Ycenter = centroids.y[0]
        
        map_all = self.create_map_template(title='Overview Map - All Overlaps',
                                    Xcenter=Xcenter,Ycenter=Ycenter)
             
        # Add the AOI layer to the all-layers map
        grp_aoi= folium.FeatureGroup(name= 'AOI')  
        lyr_aoi= folium.GeoJson(data=gdf_aoi, name='AOI',
                    style_function=lambda x:{'color': 'red', 
                                                'fillColor': 'none',
                                                'weight': 3})
        lyr_aoi.add_to(grp_aoi)
        grp_aoi.add_to(map_all)
        
        aoi_grps= [grp_aoi]
        
        # Add buffered areas to the all-layers map
        for k,v in bf_gdfs.items():
            grp_aoi_b= folium.FeatureGroup(name= k.upper()+' m')  
            lyr_aoi_b= folium.GeoJson(data=v, name=k, show=True,
                            style_function=lambda x:{'color': 'orange',
                                                     'fillColor': 'none',
                                                     'weight': 3})
            lyr_aoi_b.add_to(grp_aoi_b)
            grp_aoi_b.add_to(map_all)
        
            aoi_grps.append(grp_aoi_b)
        
        # Zoom the all-layers map to the AOI extent
        xmin,ymin,xmax,ymax = bf_gdfs.get('aoi_1000').to_crs(4326)['geometry'].total_bounds
        map_all.fit_bounds([[ymin, xmin], [ymax, xmax]])
        
        
        print ('\nGenerating Individual Maps')
        
        ctg_list= list(df_st['Category'].unique())
        #ctg_list= ['FCBC Preliminary Status', 'Archaeology and Culture', 'FCBC Admin Areas']
        ctg_list.insert(0, 'Area of Interest')
        
        ctg_grps=[aoi_grps]

        fc_list= fiona.listlayers(self.status_gdb)
        
        for ctg in ctg_list:
            print (f'\nGenerating Maps for {ctg}')
            
            df= df_st.loc[df_st['Category'] == ctg]
            fc_grps= []
            counter= 1
            for i, row in df.iterrows():  
                fc= row['Featureclass_Name(valid characters only)']
                fc= fc.replace(" ", "_")
        
                print (f"..creating Map {counter} of {len(df)}: {fc}")
                if fc in fc_list:
                    gdf_fc = gpd.read_file(filename= self.status_gdb, layer= fc)
            
                    if gdf_fc.shape[0] > 0:
                        #Flatten 3D geometries to 2D (Folium doesn't like 3D)    
                        if gdf_fc['geometry'].has_z.any():
                            gdf_fc['geometry'] = gdf_fc['geometry'].apply(
                                        lambda geom: wkt.loads(
                                            wkt.dumps(geom, output_dimension=2)))
                            
                        #convert all cols to str except geometry
                        for col in gdf_fc.columns:
                            if col != 'geometry':
                                gdf_fc[col] = gdf_fc[col].astype(str)
                            
                        # Set label column. Will be used for tooltip and legend.
                        map_title = fc.replace('_', ' ')
            
                        df_item= df_st.loc[df_st['Featureclass_Name(valid characters only)'] == map_title]
                        label_col= df_item['map_label_field'].iloc[0]
            
                        if pd.isnull(label_col):
                            label_col= df_item['Fields_to_Summarize'].iloc[0]
                        
                        if pd.isnull(label_col):
                            label_col = gdf_fc.columns[0]
                            
                        # Set pop up columns
                        popup_cols = []
                        
                        first_field = df_item['Fields_to_Summarize'].iloc[0]
                        if pd.notnull(first_field):
                            popup_cols.append(str(first_field.strip()))
                            
                        for f in range (2,7):
                            for i in df_item['Fields_to_Summarize' + str(f)].tolist():
                                if pd.notnull(i):
                                    popup_cols.append(str(i.strip()))
            
                        if len(popup_cols) == 0:
                            popup_cols = [col for col in gdf_fc.columns if col != 'geometry'] 
                        
                        # Format the popup columns for better visulization
                        for col in popup_cols:
                            gdf_fc[col] = gdf_fc[col].astype(str)
                            gdf_fc[col] = gdf_fc[col].str.wrap(width=20).str.replace('\n','<br>')
                        
                        
                        # Assign random colors to the features (for legend)
                        for x in gdf_fc.index:
                            color = np.random.randint(16, 256, size=3)
                            color = [str(hex(i))[2:] for i in color]
                            color = '#'+''.join(color).upper()
                            gdf_fc.at[x, 'color'] = color
                        
                        gdf_fc['color2']=color
                            
                        # Create an individual map
                        map_one = self.create_map_template(title=map_title,
                                                    Xcenter=Xcenter,Ycenter=Ycenter)
                        
                        # Add the AOI layer to individual maps
                        grp_aoi_o= folium.FeatureGroup(name= 'AOI')  
                        lyr_aoi_o= folium.GeoJson(data=gdf_aoi, name='AOI',
                                    style_function=lambda x:{'color': 'red', 
                                                                'fillColor': 'none',
                                                                'weight': 3})
                        lyr_aoi_o.add_to(grp_aoi_o)
                        grp_aoi_o.add_to(map_one)
            
                        aoi_grps_o= [grp_aoi_o]
            
                        # Add buffered areas to ndividual maps
                        for k,v in bf_gdfs.items():
                            grp_aoi_b_o= folium.FeatureGroup(name= k.upper()+' m')  
                            lyr_aoi_b_o= folium.GeoJson(data=v, name=k, show=True,
                                            style_function=lambda x:{'color': 'orange',
                                                                    'fillColor': 'none',
                                                                    'weight': 3})
                            lyr_aoi_b_o.add_to(grp_aoi_b_o)
                            grp_aoi_b_o.add_to(map_one)
            
                            aoi_grps_o.append(grp_aoi_b_o)
            
                        # Zoom the map to the layer extent
                        gdf_fc = gdf_fc.to_crs(4326)
                        xmin, ymin, xmax, ymax = gdf_fc['geometry'].total_bounds
                        map_one.fit_bounds([[ymin, xmin], [ymax, xmax]])
                        
                        # Create a list of columns for the tooltip
                        gdf_fc['map_title'] = map_title
                        tooltip_cols = ['map_title',label_col]
            
                        # Add the layer to the individual map
                        grp_fc_o= folium.FeatureGroup(name= map_title, show= True)  
                        lyr_fc_o= folium.GeoJson(data=gdf_fc, name=map_title,
                                    marker=folium.Circle(radius=5),
                                    style_function= lambda x: {'fillColor': x['properties']['color'],
                                                                'color': x['properties']['color'],
                                                                'weight': 2},
                                    tooltip=folium.features.GeoJsonTooltip(fields=tooltip_cols,
                                                                            aliases=['LAYER', label_col],
                                                                            labels=True),
                                    popup=folium.features.GeoJsonPopup(fields=popup_cols, 
                                                                        sticky=False,
                                                                        max_width=380))
                        lyr_fc_o.add_to(grp_fc_o)
                        grp_fc_o.add_to(map_one)
            
                        # Add the layer to the all-Layers map
                        grp_fc_a= folium.FeatureGroup(name= map_title, show= False)  
                        lyr_fc_a= folium.GeoJson(data=gdf_fc, name=map_title,
                                    marker=folium.Circle(radius=5),
                                    style_function= lambda x: {'fillColor': x['properties']['color2'],
                                                                'color': x['properties']['color2'],
                                                                'weight': 2},
                                    tooltip=folium.features.GeoJsonTooltip(fields=tooltip_cols,
                                                                            aliases=['LAYER', label_col],
                                                                            labels=True),
                                    popup=folium.features.GeoJsonPopup(fields=popup_cols, 
                                                                        sticky=False,
                                                                        max_width=380))
                        lyr_fc_a.add_to(grp_fc_a)
                        grp_fc_a.add_to(map_all)
                        
                        fc_grps.append(grp_fc_a)
            
                        # Create a Legend for individual maps
                        #legend colors and names
                        legend_labels = zip(gdf_fc['color'], gdf_fc[label_col])
                        
                        #start the div tag and set the legend size and position
                        legend_html = '''
                                    <div id="legend" style="position: fixed; 
                                    bottom: 200px; right: 30px; z-index: 1000; 
                                    background-color: #fff; padding: 10px; 
                                    border-radius: 5px; border: 1px solid grey;">
                                    '''
                                    
                        #add the AOI item to the legend
                        legend_html += '''
                                    <div style="display: inline-block; 
                                    margin-right: 10px;
                                    background-color: transparent;
                                    border: 2px solid red;
                                    width: 15px; height: 15px;"></div>AOI<br>
                                    '''
                                    
                        #add the AOI buffer item to the legend
                        legend_html += '''
                                    <div style="display: inline-block; 
                                    margin-right: 10px;background-color: transparent; 
                                    border: 2px solid orange;
                                    width: 15px; height: 15px;"></div>AOI buffers<br>
                                    '''            
                        
                        #add a header to the legend            
                        legend_html += '''
                                    <div style="font-weight: bold; 
                                    margin-bottom: 5px;">{}</div>
                                    '''.format(label_col)
                    
                        #add items to the legend
                        for color, name in legend_labels:
                            legend_html += '''
                                            <div style="display: inline-block; 
                                            margin-right: 10px;background-color: {0}; 
                                            width: 15px; height: 15px;"></div>{1}<br>
                                            '''.format(color, name)
                        #close the div tag
                        legend_html += '</div>'
            
                        #add the legend to the individual maps
                        map_one.get_root().html.add_child(folium.Element(legend_html))
            
                        # Add layer controls to the individual map
                        lyr_cont_one = folium.LayerControl()
                        lyr_cont_one.add_to(map_one)
            
                        #Add goups to the layer controls of the individual maps
                        GroupedLayerControl(
                        groups={
                        "AREA OF INTEREST": aoi_grps_o,
                        "LAYER": [grp_fc_o]
                            },
                        exclusive_groups=False,
                        collapsed=True
                            ).add_to(map_one)
                    
                        # Save the indivdiual map to html file
                        map_one.save(os.path.join(self.out_loc, fc+'.html'))
        
            
                counter += 1
        
                # Create a Legend for all-layers map
                legend_html_all = '''
                        <div id="legend" style="position: fixed; 
                        bottom: 200px; right: 30px; z-index: 1000; 
                        background-color: #fff; padding: 10px; 
                        border-radius: 5px; border: 1px solid grey;">
        
                        <div style="display: inline-block; 
                        margin-right: 10px;
                        background-color: transparent;
                        border: 2px solid red;
                        width: 15px; height: 15px;"></div>AOI<br>
                        
                        <div style="display: inline-block; 
                        margin-right: 10px;background-color: transparent; 
                        border: 2px solid orange;
                        width: 15px; height: 15px;"></div>AOI buffers<br>
                        
                        </div>
                        '''  
        
            if len(fc_grps) > 0:            
                ctg_grps.append(fc_grps) 
        
        #add the legend to the all-layers map
        map_all.get_root().html.add_child(folium.Element(legend_html_all))   
                
        # Add layer controls to the all-layers map
        lyr_cont_all = folium.LayerControl()
        lyr_cont_all.add_to(map_all)
        
        #Add status categories to the layer controls of  the all-layers map
        ctg_list = [x.upper() for x in ctg_list]
        GroupedLayerControl(
                    dict(zip(ctg_list, ctg_grps)),
                    exclusive_groups=False,
                     collapsed=False
                           ).add_to(map_all)
        
        
        
        # Save the all-layers map to html file
        print('\nGenerating the all-layers map')
        map_all.save(os.path.join(self.out_loc, '00_all_layers.html'))
        


if __name__ == "__main__":
    start_t = timeit.default_timer() #start time

    work_gdb = r'path\to\work\gdb'
    map_directory= r'path\to\maps'

    common_xls= r'path\to\statusing_input_spreadsheets\one_status_common_datasets.xlsx'
    region_xls= r'path\to\statusing_input_spreadsheets\one_status_west_coast_specific.xlsx'

    html = HTMLGenerator(common_xls, region_xls, work_gdb, map_directory)
    html.generate_html_maps()

    finish_t = timeit.default_timer() #finish time
    t_sec = round(finish_t-start_t)
    mins = int (t_sec/60)
    secs = int (t_sec%60)
    print ('\nProcessing Completed in {} minutes and {} seconds'.format (mins,secs))