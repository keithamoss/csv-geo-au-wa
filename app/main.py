import os
from flask import Flask, request, abort, jsonify, send_file, send_from_directory, make_response
import fiona
import json
import csvgeoauwa

app = Flask(__name__)
TMP_DIR = os.path.join(os.getcwd(), "tmp")

# Yes, this should be proper Python logging. One day.
class CustomLogger(object):
    def __init__(self):
        self.logs = []

    def warning(self, message, *args):
        if len(args) == 0:
            self.logs.append({
                "message": message,
            })
        else:
            self.logs.append({
                "message": message,
                "data": args
            })
    
    def clear(self):
        self.logs = []

logger = CustomLogger()

# Handle surfacing warnings for requests via Flask
@app.after_request
def after(response):
    if len(logger.logs) > 0 and response.headers["Content-type"] == "application/json":
        body = json.loads(response.get_data())
        body["warning"] = True
        body["warnings"] = logger.logs
        response.set_data(json.dumps(body))
    return response

@app.route("/hello")
def hello():
    logger.clear()
    return "Hello World from Flask (default)"

@app.route('/<version>/spatialise', methods=['POST'])
def spatialise(version):
    from StringIO import StringIO
    import pandas as pd

    logger.clear()

    # Manually enable GML support via the GDAL driver
    # Sean has good reasons for not doing so, but we'll wear the risk because our 
    # data will be pretty simple.
    import fiona
    fiona.supported_drivers.update({"GML": "rw"})

    # Grab our config object
    cfg = json.load(request.files['cfgFile'])

    # Apply converters to the CSV input to handle files with funky input
    # e.g. New lines in column values
    # Handle Windows and Unixy new line replacement
    availableConverters = {
        "removeNewLines": lambda x: x.replace("\r\n", "").replace("\r", "").replace("\n", ""),
        "replaceNewLinesWithHTMLBreak": lambda x: x.replace("\r\n", "<br>").replace("\r", "<br>").replace("\n", "<br>"),
        "replaceNewLinesWithWhitespace": lambda x: x.replace("\r\n", " ").replace("\r", " ").replace("\n", " "),
        "trimWhitespace": lambda x: x.strip()
    }

    # @NOTE: Using vetorised strings would be faster for large dataframes
    # http://stackoverflow.com/a/27535121
    converters = {}
    if "columnConverters" in cfg:
        for colName, funcName in cfg["columnConverters"].iteritems():
            if funcName in availableConverters:
                converters.update({colName: availableConverters[funcName]})
            else:
                logger.warning("Invalid converter '{}' found for column '{}'.".format(
                    funcName, colName
                ))
    #     print converters
    # return jsonify({}, 200)

    # Grab a Pandas DataFrame from our blob of request data
    df = pd.read_csv(request.files['csvFile'], encoding="iso-8859-1", converters=converters)
    
    # Drop any junk "unnamed columns"
    unnamedCols = [col for col in df.columns if col.startswith("Unnamed: ") == True]
    if len(unnamedCols) > 0:
        df = df.drop(unnamedCols, axis=1)
        
        logger.warning("Found {} unnamed columns in the dataset. These have been dropped.".format(
            len(unnamedCols)
        ))
    
    # Process the dataset according to our config rules
    try:
        dataset = csvgeoauwa.dataset()
        geoDataFrame = dataset.process(df, cfg, logger)
    except csvgeoauwa.UserException as e:
        return jsonify({
            "error": "true",
            "message": e.message
        }), 400

    return jsonify({"fileName": cfg["fileName"] + ".zip"}), 200

@app.route('/<version>/get_file/<path:filename>', methods=['GET'])
def get_file(version, filename):
    return send_from_directory(
        # app.config['UPLOAD_FOLDER'],
        TMP_DIR,
        filename, 
        as_attachment=True
    )

@app.route("/<version>/get_capabilities")
def getcapabilities(version):
    from csvgeoauwa import RegionMapping
    rm = RegionMapping()
    rm.loadRegionMapping()

    return jsonify({
        "spatialType": [
            "latlon",
            "regions"
        ],
        "drivers": fiona.supported_drivers,
        "regions": rm.regionMapping["regionsMap"],
        "converters": [
            {
                "name": "removeNewLines",
                "description": "Remove all line breaks from the contents of this column."
            },
            {
                "name": "replaceNewLinesWithHTMLBreak",
                "description": "Replace all line breaks with the HTML line break <br>."
            },
            {
                "name": "replaceNewLinesWithWhitespace",
                "description": "Replace all line breaks with a single space."
            },
            {
                "name": "trimWhitespace",
                "description": "Trim all whitespace from the beginning and end of the contents of this column."
            }
        ],
        "qaRules": [
            {
                "name": "valueConstrainedByList",
                "description": "Limit the contents of this column to a pre-defined set of values. (e.g. A list of know themes like Education, Health, Environment, ...)",
                "fields": [
                    {
                        "name": "colName",
                        "description": "Column Name",
                        "type": "column-select",
                        "multiple": False,
                        "required": True
                    },
                    {
                        "name": "validValues",
                        "description": "Values",
                        "type": "chips-textbox",
                        "required": True
                    }
                ]
            },
            {
                "name": "valueContrainedBySpatialRegion",
                "description": "Limit the contents of this column to a pre-defined set of region names. (e.g. Must be a townsite name, or the name of a local government area.)",
                "fields": [
                    {
                        "name": "colName",
                        "description": "Column Name",
                        "type": "column-select",
                        "multiple": False,
                        "required": True
                    },
                    {
                        "name": "regionId",
                        "description": "Region Name",
                        "type": "region-select",
                        "required": True
                    }
                ]
            },
            {
                "name": "valueContrainedByGeoJSON",
                "description": "Limit the contents of this dataset to a given geographic area. (e.g. All points in this dataset should be in Western Australia.)",
                "fields": [
                    {
                        "name": "geojsonFeature",
                        "description": "Geographic Area",
                        "type": "area-select-map-widget-geojson",
                        "required": True
                    }
                ]
            }
        ],
        "postProcessingRules": [
            {
                "name": "humanFriendlyRegionNames",
                "description": "Transform the contents of region names column into a human-readable representation of the name. (e.g. 'MANDURAH, CITY OF' -> 'City of Mandurah')",
                "fields": [
                    {
                        "name": "colName",
                        "description": "Column Name",
                        "type": "column-select",
                        "multiple": False,
                        "required": True
                    },
                    {
                        "name": "regionId",
                        "description": "Region Name",
                        "type": "region-select",
                        "required": True
                    }
                ]
            },
            {
                "name": "simplifyGeometry",
                "description": "Apply a simpification algorithm to the spatial boundaries of regions. (e.g. Remove complexity from a coastline dataset to reduce the size of the resulting file.)",
                "fields": [
                    {
                        "name": "tolerance",
                        "description": "Tolerance",
                        "type": "range",
                        "min": 0,
                        "max": 1,
                        "required": True
                    }
                ]
            },
            {
                "name": "dropColumns",
                "description": "Remove the given columns from the dataset after processing. (e.g. Remove the unnecessary 'lat' and 'lon' columns.)",
                "fields": [
                    {
                        "name": "colName",
                        "description": "Column Name",
                        "type": "column-select",
                        "multiple": True,
                        "required": True
                    }
                ]
            }
        ]
    })

@app.route('/ogr2ogr', methods=['POST'])
def ogr2ogr():
    # return jsonify({'foo': 'bar1'}), 200

    print request.path
    print request.method
    print request.headers['Content-Type']
    # return jsonify({'foo': 'bar2'}), 200
    # print request.data

    # How to extract features from in-memory zip files?
    # https://github.com/Toblerity/Fiona/issues/318

    with open('test.zip', 'wb') as f:
        f.write(request.data)
        f.close()
    # return jsonify({'foo': 'bar3'}), 200

    # Register format drivers with a context manager
    with fiona.drivers():
        # Open a file for reading. We'll call this the "source."
        # return jsonify({'foo': 'bar4', 'length': len(request.data)}), 200

        # https://github.com/Toblerity/Fiona/blob/master/examples/open.py
        # with fiona.open('data/CurrentActiveSchoolsSemester12015/OGRGeoJSON.shp') as source:
        with fiona.open('/CurrentActiveSchoolsSemester12015', layer='OGRGeoJSON', vfs='zip://test.zip') as source:
            os.remove('test.zip')
            return jsonify(source.meta), 200
            # print len(source)
            # print source.meta

    
    return jsonify({'error': True}), 500
    # pass

    # if not request.json or not 'title' in request.json:
    #     abort(400)
    # task = {
    #     'id': tasks[-1]['id'] + 1,
    #     'title': request.json['title'],
    #     'description': request.json.get('description', ""),
    #     'done': False
    # }
    # tasks.append(task)
    # return jsonify({'task': task}), 201

@app.route('/ogr2ogr2')
def ogr2ogr2():
    # with fiona.drivers(GEOMETRY_NAME="SHAPEY", FID="OID"):
    with fiona.drivers():
        with fiona.collection(os.path.join(TMP_DIR, "filegdb-bug/first-geom-wa-extent.json"), "r") as source:
            print source.meta
            with fiona.open(os.path.join(TMP_DIR, "test.gdb"), "w", driver="FileGDB", schema=source.meta["schema"], crs=source.meta["crs"], GEOMETRY_NAME="SHAPEY", FID="OID") as sink:
                for f in source:
                    print f
                    sink.write(f)
    return jsonify({}), 200

@app.route('/delf1fromgdb')
def delf1fromgdb():
    import geopandas as gpd
    from fiona.crs import from_epsg
    import shutil

    gdbout = os.path.join(TMP_DIR, "fish2.gdb")

    gdf = gpd.read_file(os.path.join(TMP_DIR, "fish.gdb"))
    print len(gdf)
    print gdf.crs
    gdf.crs = from_epsg(4326)
    print gdf.crs
    print

    gdf2 = gdf.iloc[1:len(gdf)]
    print gdf2

    gdf2.to_file(
        gdbout, 
        driver="FileGDB"
    )

    return jsonify({}), 200

@app.route('/ogr2ogr3')
def ogr2ogr3():
    import geopandas as gpd
    from fiona.crs import from_epsg
    import shutil

    gdbout = os.path.join(TMP_DIR, "broadscale_projects_simple.gdb")
    if os.path.exists(gdbout):
        shutil.rmtree(gdbout, ignore_errors=True)

    gdf = gpd.read_file(os.path.join(TMP_DIR, "broadscale_projects_simple_from_ago_gdb.json"))
    print len(gdf)
    print gdf.crs
    gdf.crs = from_epsg(4326)
    print gdf.crs

    gdf.to_file(
        gdbout, 
        driver="FileGDB"
    )

    return jsonify({}), 200

@app.route('/test-spatial-data')
def test_spatial_data():
    with fiona.drivers():
        with fiona.open('/data/CurrentActiveSchoolsSemester12015/OGRGeoJSON.shp', 'r') as source:
            for f in source:
                print f
            return jsonify(source.meta), 200
    return jsonify({'error': True}), 500

@app.route("/")
def main():
    return send_file('./static/index.html')

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=80)