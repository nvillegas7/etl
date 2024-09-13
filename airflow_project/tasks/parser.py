import pandas as pd
import json
from io import StringIO

def parser(raw_data):
    try:
        # Read the raw CSV data into a pandas DataFrame
        data = pd.read_csv(StringIO(raw_data))

        ## The following can be part of normalization but will keep here for simplicity
        data = data.fillna('')

        # Optionally, handle numeric columns differently (replace NaN with 0)
        numeric_cols = ['Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Incident_Rate', 'Case_Fatality_Ratio']
        for col in numeric_cols:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)

        
        # Convert the DataFrame into a list of dictionaries (one dictionary per row)
        result = data.to_dict(orient='records')
        
        print("Data parsed successfully")
        return json.dumps(result)  # Return the list of dictionaries
    except Exception as e:
        print(f"Failed to parse data: {e}")
        raise