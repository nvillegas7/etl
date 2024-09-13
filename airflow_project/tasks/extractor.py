import requests
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
from airflow.models import Variable

def extractor(parent_url=None):
    if not parent_url:
        # Default to the COVID-19 daily reports directory
        parent_url = Variable.get("covid_data_url", 
            default_var="https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports"
        )
    
    if parent_url.endswith('.csv'):
        try:
            # Fetch the CSV file directly from the URL
            response = requests.get(parent_url, verify=False)  # Disable SSL verification
            response.raise_for_status()  # Raise an error for bad requests
            data = pd.read_csv(StringIO(response.text))  # Read CSV data into a pandas DataFrame
            print("Data extracted successfully")
            return response.text  # Return the DataFrame for further processing
        
        except Exception as e:
            print(f"Failed to extract data from {url}: {e}")
            raise
    else:
        try:
            # Fetch the HTML content from the parent URL
            response = requests.get(parent_url)
            response.raise_for_status()
            
            # Parse the HTML to find links (including CSV files)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            csv_files = []
            
            # Find all <a> tags with links and convert blob URLs to raw URLs
            for link in soup.find_all('a'):
                href = link.get('href')
                # Check if it's a link to a CSV file
                if href.endswith('.csv'):
                    # Convert blob URL to raw URL
                    raw_url = href.replace('/blob/', '/raw/')
                    full_url = f"https://github.com{raw_url}"
                    csv_files.append(full_url)

            return csv_files

        except Exception as e:
            print(f"Failed to extract data from {parent_url}: {e}")
            raise
