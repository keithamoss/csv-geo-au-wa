import os
import shutil
import json
import geopandas as gpd

# from main import TMP_DIR
TMP_DIR = os.path.join(os.getcwd(), "tmp")

class dataset(object):
    def __init__(self):
        pass
    
    def process(self, df, cfg, logger):
        from StringIO import StringIO
        import pandas as pd
        import geopandas as gpd
        from fiona.crs import from_epsg
        import numpy as np

        rm = RegionMapping()

        # Support adding gids for formats that don't generate their own
        if "addGid" in cfg:
            df["gid"] = df.index
            df = df[["gid"] + list(df.columns.values)]
            print "Added an auto-incrementing gid column"

        # Allow specifying an index column that should be unique (and warn the user if not)
        # We use this column elsewhere to identify rows to the user that have warnings.
        if "indexCol" in cfg:
            df = df.set_index(cfg["indexCol"], verify_integrity=False)

            dupes = df.index.get_duplicates()
            if len(dupes) > 0:
                raise UserException("Index column contains duplicate values. '{}' has {} duplicate value(s): {}".format(
                    df.columns[0] if "indexCol" not in cfg else cfg["indexCol"], 
                    len(dupes),
                    ", ".join(dupes)
                ))
        
        # Quick and dirty QA of the data to make sure important field are as we expect them
        if "qaRules" in cfg:
            for rule in cfg["qaRules"]:
                if rule["type"] == "valueConstrainedByList":
                    nanReplace = -1
                    actualValues = df.replace(np.nan, nanReplace).groupby(rule["colName"]).size()
                    for item in actualValues.iteritems():
                        if item[0].strip() not in rule["validValues"]:
                            logger.warning("Data QA: Invalid value '{}' found for column '{}' in {} rows.".format(
                                item[0] if item[0] != nanReplace else "empty string", 
                                rule["colName"], 
                                item[1]
                            ))
                
                elif rule["type"] == "valueContrainedBySpatialRegion":
                    # Retrieve the GeoDataFrame for our given region dataset against which to validate
                    region = rm.getRegionMapping(rule["regionId"])
                    gdf = region.getGeoDataFrame()

                    # Handle any data replacements required prior to processing
                    df = region.doDataReplacements(df)
                    
                    # Create normalised lists of upper cased column values w/o null values
                    # Assumes we're always checking names of things!
                    actualValues = df[df[rule["colName"]].notnull()][rule["colName"]].str.upper().str.strip()
                    validValues = gdf[region.getSpatialProp()].str.upper()

                    invalidValues = actualValues[~actualValues.isin(validValues)].tolist()
                    if len(invalidValues) > 0:
                        logger.warning("Data QA: Invalid values found for '{}'".format(
                                rule["colName"]
                            ),
                            invalidValues
                        )
                else:
                    logger.warning("Invalid QA Rule found: {}. Skipping.".format(rule["type"]))

        # Now spatialise the data!
        if cfg["spatialType"] == "latlon":
            geoDataFrame = Spatialise().spatialisePoints(df, logger)
        elif cfg["spatialType"] == "regions" and "regionIds" in cfg:
            geoDataFrame = Spatialise().spatialiseRegions(df, logger, cfg["regionIds"])
        else:
            raise UserException("Invalid SpatialType found: {}".format(cfg["spatialType"]))
        print "Ttl Length: {}".format(len(geoDataFrame))

        # print geoDataFrame[geoDataFrame["description"].str.contains("\n") == True][["project_id", "description"]].values.tolist()
        # geoDataFrame = geoDataFrame[geoDataFrame["project_id"] == "2009-10_RGS11"]
        # geoDataFrame = geoDataFrame.iloc[0:89]
        # geoDataFrame = geoDataFrame.iloc[[0,1,88,89]]
        # print "Ttl Length: {}".format(len(geoDataFrame))

        # print geoDataFrame[["project_id"]]
        # geoDataFrame["AREA"] = geoDataFrame.area
        # geoDataFrame.sort_values("AREA", axis=0, ascending=False, inplace=True)

        # print geoDataFrame.columns
        # oldIndexName = self.getIndexName(geoDataFrame)
        # geoDataFrame.set_index("AREA", inplace=True)
        # geoDataFrame.sort_index(axis=1, ascending=False, inplace=True)
        # # print geoDataFrame.columns
        # geoDataFrame.set_index(oldIndexName, inplace=True)
        # print geoDataFrame.columns
        # geoDataFrame.drop("AREA", 1, inplace=True)
        # print geoDataFrame[["project_id"]]

        # areas = geoDataFrame.area
        # # print areas
        # print areas.nlargest(1)
        # print type(areas.nlargest(1))
        # raise UserException("Hi")

        # If we're creating FileGDBs the largest feature (by area) should be first.
        # Workaround for "addGid": true,
        if cfg["driver"] == "FileGDB":
            geoDataFrame["AREA"] = geoDataFrame.area
            geoDataFrame.sort_values("AREA", axis=0, ascending=False, inplace=True)
            geoDataFrame.drop("AREA", 1, inplace=True)

        # Apply spatial QA rules
        # e.g. Check the resulting features are within an expected area
        if "qaRulesSpatial" in cfg:
            for rule in cfg["qaRulesSpatial"]:
                if rule["type"] == "valueContrainedByGeoJSON":
                    # For simplicty we'll accept a single GeoJSON feauture for now (e.g. a Polygon)
                    feature = gpd.GeoDataFrame.from_features([rule["geojsonFeature"]])
                    featuresOutside = geoDataFrame.within(feature.unary_union).where(lambda x: x == False).dropna()

                    if len(featuresOutside) > 0:
                        invalidFeatures = []
                        indexName = self.getIndexName(geoDataFrame)
                        
                        for r in featuresOutside.iteritems():
                            invalidFeatures.append(geoDataFrame.loc[r[0]][indexName])
                        
                        logger.warning("Spatial Data QA: Found {} features outside of the expected extent.".format(
                            len(invalidFeatures)
                        ),{
                            "columnIdentifier": indexName,
                            "features": invalidFeatures
                        })
                
                else:
                    logger.warning("Invalid Spatial QA Rule found: {}. Skipping.".format(rule["type"]))

        # Apply post-processing rules
        # e.g. Convert known spatial region names to human-friendly display names
        if "postProcessingRules" in cfg:
            for rule in cfg["postProcessingRules"]:
                if rule["type"] == "humanFriendlyRegionNames":
                    region = rm.getRegionMapping(rule["regionId"])
                    regexs = region.getHumanFriendlyNameReplacement()

                    geoDataFrame = geoDataFrame.replace({rule["colName"]: {regexs[0]: regexs[1]}}, regex=True)
                    titleiser = lambda x: x if pd.isnull(x) else str(x).title()
                    geoDataFrame[rule["colName"]] = geoDataFrame[rule["colName"]].apply(titleiser)
                    print "Post-processing: Human-friendly {} names complete.".format(rule["colName"])
                
                elif rule["type"] == "simplifyGeometry":
                    # continue
                    geoDataFrame['geometry'] = geoDataFrame.simplify(rule["tolerance"])
                    print "Post-processing: Simplification complete"
                
                else:
                    logger.warning("Invalid Post-Processing Rule found: {}. Skipping.".format(rule["type"]))
        
        # from shapely.geometry import mapping

        # for index, row in geoDataFrame.iterrows():
        #     print mapping(row["geometry"])
        #     print

        # print
        # print
        # print geoDataFrame[geoDataFrame["project_id"] == "2009-10_RGS11"][["project_id", "description", "geometry"]].values.tolist()
        
        # geom = geoDataFrame[geoDataFrame["project_id"] == "2009-10_RGS11"][["geometry"]]
        # print mapping(geom)
        # print

        # newgeom = mapping(geom)
        # coordinates = newgeom["features"][0]["geometry"]["coordinates"]
        # print type(coordinates)
        # print len(coordinates)
        # print coordinates
        # print

        # print coordinates[0]
        # print

        # newgeom["features"][0]["geometry"]["coordinates"] = coordinates[0]
        # print newgeom
        # print

        # # geoDataFrame[geoDataFrame["project_id"] == "2009-10_RGS11"][["geometry"]] = newgeom
        # print geoDataFrame.columns
        # print geoDataFrame[geoDataFrame["project_id"] == "2009-10_RGS11"]
        # print

        # geoDataFrame.loc[88, 10] = {
        #     "geomemtry": newgeom,
        #     "properties": {}
        # }

        # print geoDataFrame[geoDataFrame["project_id"] == "2009-10_RGS11"][["geometry"]]
        # print 

        # geom = geom.buffer(0.1)
        # print mapping(geom)

        # print "Area: {}".format(geom.area)
        # print "Length: {}".format(geom.length)
        # print "Exterior"
        # print geom.exterior
        # print "Interiors"
        # print geom.interiors
        # print list(geom.interiors)
        # print "Bounds"
        # print geom.bounds
        # print "GeomType"
        # print geom.geom_type
        # print "IsRing"
        # print geom.is_ring
        # print "IsSimple"
        # print geom.is_simple
        # print "IsValid"
        # print geom.is_valid
        # raise UserException("Hi")

        # Drop any unnecessary columns
        if "dropColumns" in cfg:
            geoDataFrame = geoDataFrame.drop(cfg["dropColumns"], 1)
            print "Dropped unnecessary columns."

        # geoDataFrame = geoDataFrame[["gid", "project_id", "geometry"]]

        # Write out to our desired format
        if "fileName" in cfg and "driver" in cfg:
            # e.g. Write out to /app/tmp/local_projects/local_projects.mif
            outDir = os.path.join(TMP_DIR, os.path.splitext(cfg["fileName"])[0])
            if not os.path.exists(outDir):
                os.makedirs(outDir)
            
            fileOutPath = os.path.join(outDir, cfg["fileName"])
            zipfileOutPath = os.path.join(TMP_DIR, cfg["fileName"])

            # Transform to a different output CRS
            # Assume a sensible default of WGS84 if no CRS was provided.
            toCRS = from_epsg(cfg["outCRS"] if "outCRS" in cfg else "4283")
            if toCRS["init"] != geoDataFrame.crs["init"]:
                print "Changing CRS from {} to {}.".format(geoDataFrame.crs["init"], toCRS["init"])
                print "Current: {}".format(geoDataFrame.crs)
                print "New: {}".format(toCRS)
                geoDataFrame = geoDataFrame.to_crs(crs=toCRS)

            # from geopandas.io.file import infer_schema
            # print "schema"
            # print infer_schema(geoDataFrame)

            if os.path.exists(fileOutPath):
                os.remove(fileOutPath)
            if os.path.exists(zipfileOutPath + ".zip"):
                os.remove(zipfileOutPath + ".zip")

            geoDataFrame.to_file(
                fileOutPath, 
                driver=cfg["driver"]
            )
            print "GeoDataFrame written to disk {}.".format(fileOutPath)

            # Zip 'em
            shutil.make_archive(zipfileOutPath, "zip", outDir)
            print "GeoDataFrame zipped."

            # Validate that all rows were written
            geoDataFrameOnDisk = gpd.read_file(fileOutPath)
            if len(geoDataFrameOnDisk) != len(geoDataFrame):
                raise UserException("The spatial file we exported has less rows than the input source ({} vs {}). Something has gone very, very wrong here.".format(
                    len(geoDataFrame),
                    len(geoDataFrameOnDisk)
                ))

        return geoDataFrame
    
    def getIndexName(self, df):
        # Unless specifically provided, make a huge assumption that the first column is the primary key/index.
        return df.index.name if df.index.name != None else df.columns[0]


class Spatialise(object):
    def __init__(self):
        pass
    
    def spatialiseRegions(self, df, logger, regionIds):
        import pandas as pd
        import geopandas as gpd

        geoDataFrames = []
        rm = RegionMapping()

        # Run through the spatialising step for each region
        for regionId in regionIds:
            print regionId
            region = rm.getRegionMapping(regionId)
            geoDataFrames.append(self.spatialiseRegion(df, logger, region))

        # Merge our new GeoDataFrames together
        # And workaround https://github.com/geopandas/geopandas/issues/363 by manually assigning a CRS
        crs = geoDataFrames[0].crs
        mergedGeoDataFrame = gpd.GeoDataFrame(pd.concat(geoDataFrames, ignore_index=True))
        mergedGeoDataFrame.crs = crs

        return mergedGeoDataFrame

    def spatialiseRegion(self, df, logger, region):
        from shapely.geometry import Point, mapping
        from fiona import collection
        from fiona.crs import from_epsg
        import numpy as np

        # Grab a GeoPandas GeoDataFrame from our region
        gdf = region.getGeoDataFrame()

        # Hacky special case for the state boundaries (one row)
        if region.getLocatorName() == "State":
            gdf = gdf.assign(locator="State")

        # Support filtering down tabular datasets with multiple regions
        if "locator" in df.columns:
            df = df[(df["locator"] == region.getLocatorName()) & (df[region.getRegionProp()].notnull())]
            print "Filtered length by Locator: {}".format(len(df))
        
        # Harmonise the column name on the spatial data to make joining and deduping easier
        if region.getRegionProp() != region.getSpatialProp():
            gdf.rename(columns={region.getSpatialProp(): region.getRegionProp()}, inplace=True)
        
        # Discard all unnecessary props from the spatial frame
        gdf = gdf[['geometry', region.getRegionProp()]]

        # We assume the spatialProp also functions as a unique key for the tabular data, 
        # so let's do some sanity checking for duplicate rows
        dupes = gdf.set_index(region.getRegionProp()).index.get_duplicates()
        if len(dupes) > 0:
            raise UserException("Spatial dataset contains duplicate records on its unique key.<br><br>{}".format(
                dupes
            ))
        
        # Handle any data replacements required prior to processing
        df = region.doDataReplacements(df)

        # Now join our spatial and tabular data
        gdf_joined = gdf.merge(df, on=region.getRegionProp())

        # Handle any data replacements required post-processing (e.g. Human-friendly names)
        replacements = region.getPostProcessingReplacements()
        if replacements is not None:
            for replacement in replacements:
                df = df.replace({region.getRegionProp(): {replacement[0]: replacement[1]}}, regex=True)

        # Reorder columns to match that which was passed in
        gdf_joined = gdf_joined[list(df.columns.values) + ['geometry']]

        # Crudely make sure we haven't discarded any records
        if len(gdf_joined) != len(df):
            missingRows = []
            diff = df.merge(gdf_joined, how="left", indicator=True)
            for index, row in diff[diff["_merge"] == "left_only"].iterrows():
                missingRows.append("{}: {}".format(row[df.index.name], row[region.getRegionProp()]))
            
            raise UserException("The joined dataset is a different size to the input ({} vs {})<br><br>{}.".format(
                len(df), 
                len(gdf_joined),
                "<br>".join(missingRows)
            ))
        
        return gdf_joined

    def spatialisePoints(self, df, logger):
        from shapely.geometry import Point, mapping
        from fiona import collection
        from fiona.crs import from_epsg
        import numpy as np

        # Determine the coordinate column names being used
        coordinateColumns = [{"lat": "lat", "lon": "lon"}, {"lat": "y", "lon": "x"}]
        for column in coordinateColumns:
            if column["lat"] in df.columns and column["lon"] in df.columns:
                coordinateColumn = column
                break

        if coordinateColumn is None:
            raise UserException("Unable to find columns containing latitude/longitude in the dataset.")

        # Support filtering down tabular datasets with multiple regions
        if "locator" in df.columns:
            df = df[(df["locator"] == "Address or place")]
            print "Filtered length by Locator: {}".format(len(df))
        
        # Filter out any rows that are obviously invalid coordinates
        dfFiltered = df[(df[coordinateColumn["lat"]].notnull()) & (df[coordinateColumn["lon"]].notnull())]
        if len(dfFiltered) != len(df):
            logger.warning("Found {} rows with no coordinate values. These have been filtered out of the final dataset.".format(
                len(df) - len(dfFiltered)
            ))
        df = dfFiltered
        
        # Handle any data replacements required prior to processing
        # replacements = region.getDataReplacements()
        # if replacements is not None:
        #     for replacement in replacements:
        #         df = df.replace({region.getRegionProp(): {replacement[0]: replacement[1]}}, regex=True)

        # Now spatialise our data assuming that we're using WGS84
        # @TODO Check that coordinates are valid WGS84 coordinates
        geometry = [Point(xy) for xy in zip(df[coordinateColumn["lon"]], df[coordinateColumn["lat"]])]
        gdf_joined = gpd.GeoDataFrame(df, crs=from_epsg(4283), geometry=geometry)
        
        # Check for any rows with naively invalid coordinates (basically, wrong type)
        invalid = gdf_joined[~gdf_joined['geometry'].is_valid]
        if len(invalid) > 0:
            raise UserException("Invalid coordinate values found for {} rows<br><br>{}.".format(
                len(invalid), 
                invalid[[0, coordinateColumn["lat"], coordinateColumn["lon"]]]
            ))
        
        return gdf_joined


class RegionMapping(object):
    def __init__(self):
        self.REGION_MAPPING_CONFIG = "/data/regionMapping.json"

    def loadRegionMapping(self):
        if hasattr(self, "regionMapping") is False:
            with open(self.REGION_MAPPING_CONFIG) as f:
                self.regionMapping = json.load(f)
        return self.regionMapping

    def getRegionMapping(self, regionName):
        regionMapping = self.loadRegionMapping()
        if "regionsMap" in regionMapping and regionName in regionMapping["regionsMap"]:
            return Region(regionMapping["regionsMap"][regionName])
        raise Exception("No such region {}".format(regionName))


class Region(object):
    def __init__(self, config):
        self.REGION_MAPPING_DIR = "/data"
        self.config = config
    
    def getGeoDataFrame(self):
        return gpd.read_file(os.path.join(self.REGION_MAPPING_DIR, self.config["layerPath"]))
    
    def getLocatorName(self):
        return self.config["locatorName"]
    
    def getRegionProp(self):
        return self.config["regionProp"]
    
    def getSpatialProp(self):
        return self.config["spatialProp"]

    def getDataReplacements(self):
        return self.config["dataReplacements"] if "dataReplacements" in self.config else None

    def getPostProcessingReplacements(self):
        return self.config["postProcessingReplacements"] if "postProcessingReplacements" in self.config else None

    def getHumanFriendlyNameReplacement(self):
        return self.config["humanFriendlyNameReplacement"] if "humanFriendlyNameReplacement" in self.config else None
    
    def doDataReplacements(self, df):
        replacements = self.getDataReplacements()
        if replacements is not None:
            for replacement in replacements:
                df = df.replace({self.getRegionProp(): {replacement[0]: replacement[1]}}, regex=True)
        return df


class UserException(Exception):
    pass