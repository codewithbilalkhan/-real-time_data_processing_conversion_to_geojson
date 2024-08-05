import duckdb
import json
import pandas as pd

def fetch_data_from_duckdb():
    # Connect to the DuckDB database
    conn = duckdb.connect('new_data.duckdb')
    
    try:
        
        query = "SELECT * FROM device_data"
        df = conn.execute(query).df()  
    finally:
       
        conn.close()
    
    return df

def convert_to_geojson(df):
    features = []
    for _, row in df.iterrows():
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['longitude'], row['latitude']]
            },
            "properties": {
                "deviceno": row['deviceno'],
                "speed": row['speed']
            }
        }
        features.append(feature)
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    return geojson

def save_geojson(geojson, filename='data.geojson'):
    with open(filename, 'w') as f:
        json.dump(geojson, f, indent=4)

def main():
    df = fetch_data_from_duckdb()
    geojson = convert_to_geojson(df)
    save_geojson(geojson)
    print(f"GeoJSON data saved to data.geojson")

if __name__ == "__main__":
    main()
