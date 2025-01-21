map_css = """
{% macro html(this, kwargs) %}
    <style>
/* Marker PopUp Box CSS */
        .leaflet-popup-content-wrapper{
            padding: 1px;
            text-align: left;
            border: 1px solid #d7a45d;
            border-radius: 12px;

        }
        .leaflet-popup-content{
            margin: 13px 24px 13px 20px;
            font-size: 1.2em;
            line-height: 1.3;
            min-height: 1px;
        }




/* Layer Control Panel CSS */

        .leaflet-control-layers-list {
            width: 16vw;
            max-height: 350px;
            overflow-y: auto;
            overflow-x: hidden;
        }
        .leaflet-control-layers form {
            z-index: 10000;
            overflow-y: auto;
            overflow-x: hidden;
        }
        .leaflet-control-layers-group-label{
            padding: 2px;
            margin: 2px;
            background-color: #e09494;
            border: 1px dashed black;
            border-radius: 4px;
            text-align: center;
        }
    </style>

{% endmacro %}
"""