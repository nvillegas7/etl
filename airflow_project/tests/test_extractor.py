import unittest
from unittest.mock import patch
from tasks.extractor import extractor

class TestExtractor(unittest.TestCase):

    @patch('requests.get')
    def test_extractor_success(self, mock_get):
        # Mock successful response
        mock_get.return_value.status_code = 200
        mock_get.return_value.text = "column1,column2\n1,2\na,b"
        
        url = "https://example.com/test.csv"
        result = extractor(url)

        # Check if the extractor returns the expected result
        self.assertEqual(result, "column1,column2\n1,2\na,b")
    
    @patch('requests.get')
    def test_extractor_failure(self, mock_get):
        # Mock failed response
        mock_get.return_value.status_code = 404
        url = "https://example.com/invalid.csv"
        
        with self.assertRaises(Exception):
            extractor(url)

if __name__ == '__main__':
    unittest.main()
