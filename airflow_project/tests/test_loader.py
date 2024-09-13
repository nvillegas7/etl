import unittest
from unittest.mock import patch, MagicMock
from tasks.loader import loader

class TestLoader(unittest.TestCase):

    @patch('psycopg2.connect')
    def test_loader_success(self, mock_connect):
        # Mock the database connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Sample parsed data
        parsed_data = [
            {'Province_State': 'NY', 'Country_Region': 'USA', 'Lat': '40.7128', 'Long_': '-74.0060', 'Confirmed': 10000, 'Deaths': 500, 'Recovered': 8000, 'Active': 2500}
        ]

        # Call the loader function
        loader(parsed_data)

        # Check if the execute method was called with the correct SQL query
        mock_cursor.execute.assert_called()
        mock_conn.commit.assert_called_once()

    @patch('psycopg2.connect')
    def test_loader_failure(self, mock_connect):
        # Mock the connection failure
        mock_connect.side_effect = Exception("Database connection failed")
        
        parsed_data = [
            {'Province_State': 'NY', 'Country_Region': 'USA', 'Lat': '40.7128', 'Long_': '-74.0060', 'Confirmed': 10000, 'Deaths': 500, 'Recovered': 8000, 'Active': 2500}
        ]

        with self.assertRaises(Exception):
            loader(parsed_data)

if __name__ == '__main__':
    unittest.main()
