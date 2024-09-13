import json
import psycopg2

def loader(parsed_data_json):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host="postgres",
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()

        # Insert each row (dictionary) into the database
        for row in json.loads(parsed_data_json):
            # Convert the additional data to a JSON string
            additional_data = {k: v for k, v in row.items() if k not in [
                'Province_State', 'Country_Region', 'Lat', 'Long_', 'Confirmed', 
                'Deaths', 'Recovered', 'Active', 'Incident_Rate', 'Case_Fatality_Ratio', 'Last_Update'
            ]}
            additional_data_json = json.dumps(additional_data)

            # Replace empty strings with None for numeric columns
            cursor.execute("""
                INSERT INTO covid_raw_data (
                    province_state, country_region, lat, long, Confirmed, Deaths, Recovered, Active, 
                    Incident_Rate, Case_Fatality_Ratio, last_update, additional_data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """, (
                row.get('Province_State', 'N/A'), 
                row.get('Country_Region', 'N/A'),
                float(row.get('Lat')) if row.get('Lat') != '' else None,  # Handle numeric columns
                float(row.get('Long_')) if row.get('Long_') != '' else None,  # Handle numeric columns
                int(row.get('Confirmed', 0)), 
                int(row.get('Deaths', 0)),
                int(row.get('Recovered', 0)), 
                int(row.get('Active', 0)),
                float(row.get('Incident_Rate', 0)),
                float(row.get('Case_Fatality_Ratio', 0)),
                row.get('Last_Update', None), 
                additional_data_json  # Pass the JSON string to PostgreSQL
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print("Data loaded successfully into PostgreSQL")

    except Exception as e:
        print(f"Failed to load data into PostgreSQL: {e}")
        raise
