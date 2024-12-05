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
    "Snow_Priority","gridcode","OBJECTID","Shape","SHAPE"
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
            cell_size=10,
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
    except Exception as e:
        logging.error(f"Error in raster_to_polygon: {e}")
        raise


def polygon_to_line(input_polygon, output_line):
    try:
        logging.info("Step 5: Converting polygons to lines...")
        arcpy.management.PolygonToLine(
            in_features=input_polygon,
            out_feature_class=output_line,
            neighbor_option="IGNORE_NEIGHBORS",
        )
        logging.info(f"Lines created: {output_line}")
    except Exception as e:
        logging.error(f"Error in polygon_to_line: {e}")
        raise


def spatial_join(target_features, join_features, output_fc):
    """
    Perform spatial join between target_features and join_features.
    """
    try:
        logging.info("Step 6: Performing spatial join...")

        # Log fields in the target and join features
        target_fields = [field.name for field in arcpy.ListFields(target_features)]
        join_fields = [field.name for field in arcpy.ListFields(join_features)]
        logging.info(f"Fields in target_features ({target_features}): {target_fields}")
        logging.info(f"Fields in join_features ({join_features}): {join_fields}")

        # Perform the spatial join
        arcpy.analysis.SpatialJoin(
            target_features=target_features,
            join_features=join_features,
            out_feature_class=output_fc,
            join_type="KEEP_ALL",
        )
        logging.info(f"Spatial join completed: {output_fc}")

        # Log the record count
        count = int(arcpy.management.GetCount(output_fc)[0])
        logging.info(f"Spatial join output contains {count} features.")
        if count == 0:
            raise ValueError("Spatial join resulted in zero features. Check input data alignment.")

        # Log fields in the output feature class
        output_fields = [field.name for field in arcpy.ListFields(output_fc)]
        logging.info(f"Fields in output feature class ({output_fc}): {output_fields}")

        # Delete unwanted fields
        if fields_to_keep:
            fields_to_delete = [f for f in output_fields if f not in fields_to_keep + ["OBJECTID", "Shape"]]
            if fields_to_delete:
                arcpy.management.DeleteField(output_fc, fields_to_delete)
                logging.info(f"Deleted fields: {fields_to_delete}")
            else:
                logging.info("No fields deleted. All required fields are present.")

    except Exception as e:
        logging.error(f"Error in spatial_join: {e}")
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
        polygon_to_line(polygons, lines)

        # Perform spatial join with LION dataset
        lion_feature_class = os.path.join(LION_GDB, "LION")  # Replace with your LION feature class name
        joined_features = os.path.join(OUTPUT_GDB, "lion_joined_features")
        spatial_join(lion_feature_class, lines, joined_features)

        # Log the record count
        count = int(arcpy.management.GetCount(joined_features)[0])
        logging.info(f"Spatial join completed with {count} features.")
        if count == 0:
            raise ValueError("Spatial join resulted in zero features. Check input data alignment.")

        # Verify the data in joined_features
        if int(arcpy.management.GetCount(joined_features)[0]) == 0:
            logging.error("No features found in the joined_features dataset. GeoJSON export aborted.")
            raise ValueError("No features available in joined_features.")


        # Export to GeoJSON
        temp_geojson = f"C:\\Users\\cisuser\\Documents\\research\\temp_output.geojson"
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
        logging.info(f"GeoJSON uploaded to table '{table_name}' successfully.")    

        logging.info("Workflow completed successfully!")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
