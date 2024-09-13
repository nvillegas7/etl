import unittest
from tasks.parser import parser

class TestParser(unittest.TestCase):

    def test_parser_success(self):
        # Sample CSV data
        raw_data = "column1,column2,column3\n1,2,3\na,,c"

        # Expected output
        expected_output = [
            {'column1': '1', 'column2': '2', 'column3': '3'},
            {'column1': 'a', 'column2': '', 'column3': 'c'}
        ]

        result = parser(raw_data)
        self.assertEqual(result, expected_output)

    def test_parser_empty_data(self):
        # Empty CSV data
        raw_data = ""

        # Expected output
        expected_output = []

        result = parser(raw_data)
        self.assertEqual(result, expected_output)

if __name__ == '__main__':
    unittest.main()
