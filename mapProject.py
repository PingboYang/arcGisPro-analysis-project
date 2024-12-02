import arcpy
import os

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


# Set workspace and output paths
DATABASE_CONNECTION = r"C:\Users\cisuser\Documents\research\my_database_connection.sde"
OUTPUT_GDB = r"C:\Users\cisuser\Documents\research\collision_points.gdb"

# Ensure the output geodatabase exists
if not arcpy.Exists(OUTPUT_GDB):
    arcpy.CreateFileGDB_management(os.path.dirname(OUTPUT_GDB), os.path.basename(OUTPUT_GDB))

arcpy.env.workspace = OUTPUT_GDB
arcpy.env.overwriteOutput = True

def create_points():
    """Create XY points from database."""
    print("Step 1: Creating XY points...")
    input_table = f"{DATABASE_CONNECTION}/mygisdb.public.collisiondata_20241104_to_20241118"
    output_fc = os.path.join(OUTPUT_GDB, "collision_points")
    spatial_reference = arcpy.SpatialReference(4326)

    # Use the input table to directly create points
    arcpy.management.XYTableToPoint(
        in_table=input_table,
        out_feature_class=output_fc,
        x_field="longitude",
        y_field="latitude",
        z_field=None,
        coordinate_system=spatial_reference,
    )

    print(f"XY Point data successfully exported to {output_fc}")
    return output_fc

def kernel_density(input_points, output_density):
    """Perform Kernel Density analysis."""
    print("Step 2: Performing Kernel Density analysis...")
    arcpy.sa.KernelDensity(
        in_features=input_points,
        population_field="EPDO",
        cell_size=10,
        search_radius=100,
        out_cell_values="DENSITIES",
        method="PLANAR",
    ).save(output_density)
    print(f"Kernel Density analysis completed: {output_density}")

def reclassify_density(input_raster, output_reclass):
    """Reclassify Kernel Density raster into 9 classes."""
    print("Step 3: Reclassifying density...")
    reclass_field = "Value"
    remap = arcpy.sa.RemapRange([[0, 10, 1], [10, 20, 2], [20, 30, 3], [30, 40, 4], [40, 50, 5], [50, 60, 6], [60, 70, 7], [70, 80, 8], [80, 90, 9]])
    arcpy.sa.Reclassify(input_raster, reclass_field, remap, "NODATA").save(output_reclass)
    print(f"Reclassification completed: {output_reclass}")

def raster_to_polygon(input_raster, output_polygon):
    """Convert raster to polygon."""
    print("Step 4: Converting raster to polygon...")
    arcpy.conversion.RasterToPolygon(
        in_raster=input_raster,
        out_polygon_features=output_polygon,
        simplify="SIMPLIFY",
        raster_field="VALUE",
    )
    print(f"Polygon created: {output_polygon}")

def polygon_to_line(input_polygon, output_line):
    """Convert polygons to lines."""
    print("Step 5: Converting polygons to lines...")
    arcpy.management.PolygonToLine(
        in_features=input_polygon,
        out_feature_class=output_line,
        neighbor_option="IGNORE_NEIGHBORS",
    )
    print(f"Lines created: {output_line}")

def spatial_join(target_features, join_features, output_fc):
    """Perform spatial join."""
    print("Step 6: Performing spatial join...")
    arcpy.analysis.SpatialJoin(
        target_features=target_features,
        join_features=join_features,
        out_feature_class=output_fc,
        join_type="KEEP_COMMON",
    )
    print(f"Spatial join completed: {output_fc}")

def export_to_geojson(input_features, output_geojson):
    """Export feature class to GeoJSON."""
    print("Step 7: Exporting to GeoJSON...")
    arcpy.FeaturesToJSON_conversion(input_features, output_geojson, geoJSON="GEOJSON")
    print(f"GeoJSON created: {output_geojson}")

# Main workflow
if __name__ == "__main__":
    try:
        points = create_points()
        density_raster = os.path.join(OUTPUT_GDB, "kernel_density")
        kernel_density(points, density_raster)

        reclassified_raster = os.path.join(OUTPUT_GDB, "reclassified_density")
        reclassify_density(density_raster, reclassified_raster)

        polygons = os.path.join(OUTPUT_GDB, "density_polygons")
        raster_to_polygon(reclassified_raster, polygons)

        lines = os.path.join(OUTPUT_GDB, "density_lines")
        polygon_to_line(polygons, lines)

        joined_features = os.path.join(OUTPUT_GDB, "joined_features")
        spatial_join(lines, polygons, joined_features)

        geojson_output = r"C:\Users\cisuser\Documents\research\output.json"
        export_to_geojson(joined_features, geojson_output)

        print("Workflow completed successfully!")
    except Exception as e:
        print(f"An error occurred: {e}")

