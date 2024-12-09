import arcpy
import os
import logging
import datetime
import psycopg2
import json



print(arcpy.GetInstallInfo())
#only test the xy table
# # # Set parameters for the database connection
# # arcpy.CreateDatabaseConnection_management(
# #     out_folder_path=r"C:\Users\cisuser\Documents\research",  # Folder to save the .sde file
# #     out_name="my_database_connection.sde",                  # Name of the connection file
# #     database_platform="POSTGRESQL",                         # Database platform
# #     instance="postgres-1.ctfojr4mfu0j.us-east-1.rds.amazonaws.com",  # Instance (hostname)
# #     account_authentication="DATABASE_AUTH",                 # Authentication type
# #     username="postgres",                                    # Username
# #     password="bmcccis##",                                   # Password
# #     save_user_pass="SAVE_USERNAME",                         # Save username and password
# #     database="mygisdb"                                    # Replace with your user-defined database name
# # )
 
# # Path to your SDE connection file
# database_connection = r"C:\Users\cisuser\Documents\research\my_database_connection.sde"
# arcpy.env.workspace = database_connection
# print("Database connection created successfully!")

# # Fully qualified table name
# table_name = "mygisdb.public.collisiondata_20241104_to_20241118"

# # Output feature class path
# output_fc = r"C:\Users\cisuser\Documents\research\collision_points.gdb\collision_points"  # Ensure this points to a valid geodatabase

# # Ensure the output geodatabase exists
# if not arcpy.Exists(r"C:\Users\cisuser\Documents\research\collision_points.gdb"):
#     arcpy.CreateFileGDB_management(r"C:\Users\cisuser\Documents\research", "collision_points.gdb")

# # Define the spatial reference (e.g., WGS 1984)
# spatial_reference = arcpy.SpatialReference(4326)

# # Create XY points from the database table
# arcpy.management.XYTableToPoint(
#     in_table=table_name,
#     out_feature_class=output_fc,
#     x_field="longitude",  # Replace with the actual field name
#     y_field="latitude",   # Replace with the actual field name
#     z_field=None,
#     coordinate_system=spatial_reference
# )

# print(f"XY Point data successfully exported to {output_fc}")
####################################################################


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Print ArcGIS installation info
logging.info(arcpy.GetInstallInfo())


# Set workspace and output paths
DATABASE_CONNECTION = r"C:\Users\cisuser\Documents\research\mapProject\my_database_connection.sde"
OUTPUT_GDB = r"C:\Users\cisuser\Documents\research\collision_points.gdb"
LION_GDB= r"C:\Users\cisuser\Documents\research\lion\lion.gdb"


# Fields to retain during the spatial join
fields_to_keep = [
    "Street", "FeatureTyp", "TrafDir", "StreetCode", "LZip", "LBoro", 
    "RW_TYPE", "Status", "StreetWidth_Min", "BikeLane", "BIKE_TRAFDIR", 
    "POSTED_SPEED", "SHAPE_Length", "StreetWidth_Max", "LCB2020", 
    "Snow_Priority","gridcode","OBJECTID","Shape","SHAPE","gridcode_1"
]

# Ensure the output geodatabase exists
if not arcpy.Exists(OUTPUT_GDB):
    arcpy.CreateFileGDB_management(os.path.dirname(OUTPUT_GDB), os.path.basename(OUTPUT_GDB))

arcpy.env.workspace = OUTPUT_GDB
arcpy.env.overwriteOutput = True
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(4326)




def create_points():
    try:
        logging.info("Step 1: Creating XY points...")
        input_table = f"{DATABASE_CONNECTION}/mygisdb.public.collisiondata_20241104_to_20241118"
        output_fc = os.path.join(OUTPUT_GDB, "collision_points")
        spatial_reference = arcpy.SpatialReference(4326)

        arcpy.management.XYTableToPoint(
            in_table=input_table,
            out_feature_class=output_fc,
            x_field="longitude",
            y_field="latitude",
            z_field=None,
            coordinate_system=spatial_reference,
        )
        logging.info(f"XY Point data successfully exported to {output_fc}")
        return output_fc
    except Exception as e:
        logging.error(f"Error in create_points: {e}")
        raise


def kernel_density(input_points, output_density):
    try:
        logging.info("Step 2: Performing Kernel Density analysis...")
        kernel_raster = arcpy.sa.KernelDensity(
            in_features=input_points,
            population_field="EPDO",
            cell_size=5,
            search_radius=100,
            out_cell_values="DENSITIES",
            method="PLANAR",
        )
        kernel_raster.save(output_density)

        # Log minimum and maximum values
        min_value = float(arcpy.GetRasterProperties_management(output_density, "MINIMUM").getOutput(0))
        max_value = float(arcpy.GetRasterProperties_management(output_density, "MAXIMUM").getOutput(0))
        logging.info(f"Kernel Density Min: {min_value}, Max: {max_value}")

        logging.info(f"Kernel Density analysis completed: {output_density}")
        return min_value, max_value  # Return min and max values for further use
    except Exception as e:
        logging.error(f"Error in kernel_density: {e}")
        raise



def reclassify_density(input_raster, output_reclass, min_value, max_value):
    try:
        logging.info("Step 3: Reclassifying density...")

        # Define dynamic remap range based on min and max values
        num_classes = 9
        step = (max_value - min_value) / num_classes
        remap = arcpy.sa.RemapRange([
            [min_value + i * step, min_value + (i + 1) * step, i + 1]
            for i in range(num_classes)
        ])

        arcpy.sa.Reclassify(input_raster, "Value", remap, "NODATA").save(output_reclass)
        logging.info(f"Reclassification completed: {output_reclass}")

        # Check if the reclassified raster has gridcode values
        unique_values = arcpy.sa.ZonalStatisticsAsTable(
            output_reclass, "Value", input_raster, "in_memory/zone_stats", "NODATA", "ALL"
        )
        logging.info(f"Unique gridcode values in reclassified raster: {[row[0] for row in arcpy.da.SearchCursor(unique_values, ['Value'])]}")
    except Exception as e:
        logging.error(f"Error in reclassify_density: {e}")
        raise


def raster_to_polygon(input_raster, output_polygon):
    try:
        logging.info("Step 4: Converting raster to polygon...")
        arcpy.conversion.RasterToPolygon(
            in_raster=input_raster,
            out_polygon_features=output_polygon,
            simplify="SIMPLIFY",
            raster_field="VALUE",
        )
        logging.info(f"Polygon created: {output_polygon}")

        # Check gridcode values in the polygon feature class
        gridcode_values = [row[0] for row in arcpy.da.SearchCursor(output_polygon, ["gridcode"])]
        logging.info(f"Gridcode values in polygons: {gridcode_values}")
    except Exception as e:
        logging.error(f"Error in raster_to_polygon: {e}")
        raise


def polygon_to_line(input_polygon, output_line, reprojected_line):
    try:
        logging.info("Step 5: Converting polygons to lines...")
        arcpy.management.PolygonToLine(input_polygon, output_line, "IGNORE_NEIGHBORS")
        logging.info(f"Lines created: {output_line}")

        # Reproject lines only if necessary
        lion_spatial_ref = arcpy.Describe(os.path.join(LION_GDB, "LION")).spatialReference
        if arcpy.Describe(output_line).spatialReference.name != lion_spatial_ref.name:
            arcpy.management.Project(output_line, reprojected_line, lion_spatial_ref)
            logging.info(f"Lines reprojected to: {lion_spatial_ref.name}")
            return reprojected_line

        return output_line
    except Exception as e:
        logging.error(f"Error in polygon_to_line: {e}")
        raise



def spatial_join(target_features, join_features, output_fc):
    try:
        logging.info("Step 6: Performing spatial join...")

        # Configure field mappings
        field_mappings = arcpy.FieldMappings()
        field_mappings.addTable(target_features)
        field_mappings.addTable(join_features)

        # Explicitly map `gridcode`
        gridcode_map = arcpy.FieldMap()
        gridcode_map.addInputField(join_features, "gridcode")
        gridcode_field = gridcode_map.outputField
        gridcode_field.name = "gridcode"
        gridcode_map.outputField = gridcode_field
        field_mappings.addFieldMap(gridcode_map)

        # Perform the spatial join
        arcpy.analysis.SpatialJoin(
            target_features,
            join_features,
            out_feature_class=output_fc,
            join_type="KEEP_ALL",
            field_mapping=field_mappings,
        )
        logging.info(f"Spatial join completed: {output_fc}")

        # Log unique gridcode values in output
        check_gridcode_values(output_fc, "gridcode")
    except Exception as e:
        logging.error(f"Error in spatial_join: {e}")
        raise


def second_spatial_join(joined_features, lines, final_output_fc):
    """
    Perform a second spatial join between the already joined features and the lines.
    """
    try:
        logging.info("Step 7: Performing second spatial join...")

        # Check spatial references and reproject if needed
        target_sr = arcpy.Describe(joined_features).spatialReference
        join_sr = arcpy.Describe(lines).spatialReference

        if target_sr.name != join_sr.name:
            logging.info("Reprojecting lines to match the joined_features spatial reference...")
            reprojected_lines = os.path.join(OUTPUT_GDB, "reprojected_lines")
            arcpy.management.Project(lines, reprojected_lines, target_sr)
            lines = reprojected_lines
            logging.info(f"Reprojected lines saved to: {lines}")

        # Perform the spatial join
        arcpy.analysis.SpatialJoin(
            target_features=joined_features,
            join_features=lines,
            out_feature_class=final_output_fc,
            join_type="KEEP_ALL",
        )
        logging.info(f"Second spatial join completed: {final_output_fc}")

        # Log the record count and fields in the final output
        count = int(arcpy.management.GetCount(final_output_fc)[0])
        logging.info(f"Second spatial join output contains {count} features.")
        if count == 0:
            raise ValueError("Second spatial join resulted in zero features. Check input data alignment.")

        output_fields = [field.name for field in arcpy.ListFields(final_output_fc)]
        logging.info(f"Fields in final output feature class ({final_output_fc}): {output_fields}")

        # Log gridcode values in the final output
        with arcpy.da.SearchCursor(final_output_fc, ["gridcode"]) as cursor:
            gridcode_values = {row[0] for row in cursor}
        logging.info(f"Gridcode values in final output: {gridcode_values}")

    except Exception as e:
        logging.error(f"Error in second_spatial_join: {e}")
        raise


def configure_field_mapping(target, join, output_fc):
    """
    Configures field mappings to ensure 'gridcode' is transferred.
    """
    field_mappings = arcpy.FieldMappings()

    # Add target fields
    target_fm = arcpy.FieldMap()
    target_fm.addInputField(target, "gridcode")
    target_fm.mergeRule = "First"
    field_mappings.addFieldMap(target_fm)

    # Add join fields
    join_fm = arcpy.FieldMap()
    join_fm.addInputField(join, "gridcode")
    join_fm.mergeRule = "First"
    field_mappings.addFieldMap(join_fm)

    # Perform the spatial join
    arcpy.analysis.SpatialJoin(target, join, output_fc, "KEEP_ALL", "", field_mappings)
    logging.info(f"Field mapping configured and spatial join completed: {output_fc}")


def check_gridcode_values(feature_class, field_name):
    """
    Logs unique values in the specified field of a feature class.
    """
    try:
        with arcpy.da.SearchCursor(feature_class, [field_name]) as cursor:
            values = {row[0] for row in cursor if row[0] is not None}
        logging.info(f"Unique values in '{field_name}' for {feature_class}: {values}")
    except Exception as e:
        logging.error(f"Error checking field '{field_name}' in {feature_class}: {e}")
        raise



 


def export_to_geojson(input_features, output_geojson):
    """Export feature class to GeoJSON."""
    logging.info("Step 7: Exporting to GeoJSON...")
    count = int(arcpy.management.GetCount(input_features)[0])
    if count == 0:
        logging.error("Input features for GeoJSON export are empty. Aborting.")
        raise ValueError("Input features are empty.")

    arcpy.FeaturesToJSON_conversion(input_features, output_geojson, geoJSON="GEOJSON")
    logging.info(f"GeoJSON created: {output_geojson}")


def upload_geojson_to_db(geojson_data, db_params):
    """
    Upload GeoJSON data directly to a PostgreSQL database with a dynamic table name.
    Args:
        geojson_data (dict): GeoJSON data as a dictionary.
        db_params (dict): Dictionary containing database connection parameters.
    """
    try:
        # Generate a unique table name based on the current date and time
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        table_name = f"geojson_data_{timestamp}"
        
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=db_params['dbname'],
            user=db_params['user'],
            password=db_params['password'],
            host=db_params['host'],
            port=db_params['port']
        )
        cur = conn.cursor()
        logging.info(f"Connected to the database: {db_params['dbname']}")

        # Create a new table for the GeoJSON data
        logging.info(f"Creating table '{table_name}'...")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                properties JSONB,
                geometry GEOMETRY(geometry, 4326)
            );
        """)
        conn.commit()
        logging.info(f"Table '{table_name}' created successfully.")

        # Insert GeoJSON features into the table
        logging.info("Inserting GeoJSON data into the table...")
        for feature in geojson_data['features']:
            properties = feature.get('properties', {})
            geometry = json.dumps(feature.get('geometry'))
            cur.execute(f"""
                INSERT INTO {table_name} (properties, geometry)
                VALUES (%s, ST_GeomFromGeoJSON(%s));
            """, (json.dumps(properties), geometry))

        conn.commit()
        logging.info(f"GeoJSON data successfully uploaded to table '{table_name}'.")
        return table_name  # Return the table name for reference
    except Exception as e:
        logging.error(f"Error uploading GeoJSON to the database: {e}")
        raise
    finally:
        cur.close()
        conn.close()    



# Main workflow
if __name__ == "__main__":
    try:
        points = create_points()
        density_raster = os.path.join(OUTPUT_GDB, "kernel_density")

        # Perform Kernel Density analysis and get min/max values
        min_value, max_value = kernel_density(points, density_raster)


        reclassified_raster = os.path.join(OUTPUT_GDB, "reclassified_density")
        reclassify_density(density_raster, reclassified_raster, min_value, max_value)

        polygons = os.path.join(OUTPUT_GDB, "density_polygons")
        raster_to_polygon(reclassified_raster, polygons)

        lines = os.path.join(OUTPUT_GDB, "density_lines")
        reprojected_lines = os.path.join(OUTPUT_GDB, "density_lines_reprojected")
        lines = polygon_to_line(polygons, lines, reprojected_lines)

        # Perform spatial join with LION dataset
        lion_feature_class = os.path.join(LION_GDB, "LION")  # Replace with your LION feature class name
        joined_features = os.path.join(OUTPUT_GDB, "lion_joined_features")
        spatial_join(lion_feature_class, lines, joined_features)

        gridcode_values = {row[0] for row in arcpy.da.SearchCursor(joined_features, ["gridcode_1"])}
        logging.info(f"Gridcode_1 values in joined features: {sorted(gridcode_values)}")

        ''' # Log the record count
        count = int(arcpy.management.GetCount(joined_features)[0])
        logging.info(f"Spatial join completed with {count} features.")
        if count == 0:
            raise ValueError("Spatial join resulted in zero features. Check input data alignment.")

        # Verify the data in joined_features
        if int(arcpy.management.GetCount(joined_features)[0]) == 0:
            logging.error("No features found in the joined_features dataset. GeoJSON export aborted.")
            raise ValueError("No features available in joined_features.")'''
        
        # Perform the second spatial join
        final_output_fc = os.path.join(OUTPUT_GDB, "final_joined_features")
        second_spatial_join(joined_features, lines, final_output_fc)

        '''# First spatial join with explicit field mapping
        configure_field_mapping(lion_feature_class, lines, joined_features)
        check_gridcode_values(joined_features, "gridcode")

        # Second spatial join
        configure_field_mapping(joined_features, lines, final_output_fc)
        check_gridcode_values(final_output_fc, "gridcode")'''
        

        '''     # Verify the data in joined_features
        gridcode_values = {row[0] for row in arcpy.da.SearchCursor(joined_features, ["gridcode"])}
        logging.info(f"Gridcode values in joined features: {sorted(gridcode_values)}")'''

        # Verify the data in joined_features
        gridcode_values = {row[0] for row in arcpy.da.SearchCursor(final_output_fc, ["gridcode"])}
        logging.info(f"Gridcode values in final features: {sorted(gridcode_values)}")

        gridcode_1values = {row[0] for row in arcpy.da.SearchCursor(final_output_fc, ["gridcode_1"])}
        logging.info(f"Gridcode_1 values in final features: {sorted(gridcode_1values)}")

        gridcode_12values = {row[0] for row in arcpy.da.SearchCursor(final_output_fc, ["gridcode_12"])}
        logging.info(f"Gridcode_12 values in final features: {sorted(gridcode_12values)}")


        # Export to GeoJSON
        '''temp_geojson = f"C:\\Users\\cisuser\\Documents\\research\\temp_output.geojson"
        export_to_geojson(joined_features, temp_geojson)

        with open(temp_geojson, "r") as geojson_file:
            geojson_data=json.load(geojson_file)

        # Upload GeoJSON data to the database
        db_params = {
            "dbname": "mygisdb",
            "user": "postgres",
            "password": "bmcccis##",
            "host": "postgres-1.ctfojr4mfu0j.us-east-1.rds.amazonaws.com",
            "port": "5432"
        }
        table_name = upload_geojson_to_db(geojson_data, db_params)
        logging.info(f"GeoJSON uploaded to table '{table_name}' successfully.") '''   

        logging.info("Workflow completed successfully!")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
