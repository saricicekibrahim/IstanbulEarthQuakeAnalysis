import pandas as pd
import geopandas as gpd
from shapely.geometry import shape
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import sys

def main():
    # this is the name of the geography you want to retrieve. Update to meet your needs.
    location = 'Turkey'

    # link may have changed
    dataset_links = pd.read_csv("https://minedbuildings.blob.core.windows.net/global-buildings/dataset-links.csv")
    location_links = dataset_links[dataset_links.Location == location]
    
    # Modify with your database credentials
    db_user = 'db_user'
    db_password = 'db_password'
    db_host = 'db_host'
    db_port = 'db_port'
    db_name = 'db_name'
    
    # Specify the table name
    table_name = 'm_buildings'
    schema = 'public'  # Change this if you have a different schema
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

    # Create a SQLAlchemy engine
    try:
        engine = create_engine(connection_string)
        print("Database engine created successfully")
    except Exception as e:
        print(f"Error creating engine: {e}")
        sys.exit(1)

    for i, row in location_links.iterrows():
        print(f"Processing {i+1}/{len(location_links)}: {row.Url}")

        df = pd.read_json(row.Url, lines=True)
        df['geometry'] = df['geometry'].apply(shape)
        gdf = gpd.GeoDataFrame(df, crs=4326)

        # Rename 'geometry' column to 'geom' which matches the PostGIS table's schema
        gdf = gdf.rename(columns={'geometry': 'geom'})
        gdf.set_geometry('geom', inplace=True)
        
        # Reproject GeoDataFrame to EPSG:3857
        gdf = gdf.to_crs(epsg=3857)

        gdf = gdf[['geom']]  # Ensure that only the 'geom' column is preserved

        try:
            # Append data to the PostGIS table with appropriate column mapping
            gdf.to_postgis(name=table_name, con=engine, schema=schema, if_exists='append', index=False)
            print(f"Data for {row.Url} appended to PostGIS table {table_name}")
        except Exception as e:
            print(f"Error appending data to table {table_name}: {e}")
    
    try:
        with engine.connect() as connection:
            alter_table_sql_1 = text(f'begin;ALTER TABLE {schema}.{table_name} ADD COLUMN id bigserial PRIMARY KEY;')
            alter_table_sql_2 = text(f'ALTER TABLE {schema}.{table_name} ADD COLUMN osm boolean DEFAULT false;')
            alter_table_sql_3 = text(f'update {schema}.{table_name} set osm = true from import.osm_buildings where st_intersects(geom,st_transform(import.osm_buildings.geometry, 3857));commit;')
            connection.execute(alter_table_sql_1)
            connection.execute(alter_table_sql_2)
            connection.execute(alter_table_sql_3)
            print(f"Alter table commands executed successfully on {table_name}")
    except SQLAlchemyError as e:
        print(f"Error executing alter table commands: {e}")

if __name__ == "__main__":
    main()