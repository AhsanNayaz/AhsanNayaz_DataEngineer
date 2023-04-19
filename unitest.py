import unittest
import os
import csv
from io import StringIO
import xml.etree.ElementTree as et
from unittest.mock import patch, mock_open
from processing import *


class TestMyModule(unittest.TestCase):

    def test_download_xml(self):
        # Test downloading XML from a valid URL
        url = "https://www.w3schools.com/xml/note.xml"
        with open('sample.xml', 'w', encoding='utf-8') as f:
            f.write('''<note><to>Tove</to><from>Jani</from><heading>Reminder</heading><body>Don't forget me this weekend!</body></note>''')
        expected_content = 'sample.xml'
        with patch('requests.get') as mock_get:
            mock_get.return_value.content = expected_content
            result = download_xml(url)
            mock_get.assert_called_once_with(url)
            self.assertEqual(result, expected_content)

        # Test handling of HTTP request errors
        with patch('requests.get') as mock_get:
            mock_get.side_effect = requests.exceptions.RequestException()
            with self.assertRaises(requests.exceptions.RequestException):
                download_xml(url)

        # Test handling of unexpected errors
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception()
            with self.assertRaises(Exception):
                download_xml(url)
        os.remove('sample.xml')
        
    def test_parse_xml(self):
        # Test parsing valid XML content
        xml_content = b'<xml><data>example data</data></xml>'
        expected_root = et.fromstring(xml_content)
        result = parse_xml(xml_content)
        self.assertEqual(result.tag, expected_root.tag)
        self.assertEqual(result.text, expected_root.text)

        # Test handling of XML parsing errors
        xml_content = b'<xml><data>example data<'
        with self.assertRaises(et.ParseError):
            parse_xml(xml_content)

    def test_find_download_link(self):
        # Test finding download link in XML root
        xml_content = '<root><str name="download_link">https://example.com/xml_data.zip</str></root>'
        xml_root = et.fromstring(xml_content)
        expected_result = 'https://example.com/xml_data.zip'
        result = find_download_link(xml_root)
        self.assertEqual(result, expected_result)

    def test_download_zip(self):
        # Test downloading a ZIP file from a valid URL
        url = 'https://getsamplefiles.com/download/zip/sample-1.zip'
        zip_file = download_zip(url)
        self.assertIsInstance(zip_file, zipfile.ZipFile)

        # Test handling of HTTP request errors
        with patch('requests.get') as mock_get:
            mock_get.side_effect = requests.exceptions.RequestException()
            with self.assertRaises(requests.exceptions.RequestException):
                download_zip(url)

        # Test handling of unexpected errors
        with patch('requests.get') as mock_get:
            mock_get.side_effect = Exception()
            with self.assertRaises(Exception):
                download_zip(url)

    def test_extract_xml_from_zip(self):
        zip_file = zipfile.ZipFile('test.zip', 'w')
        zip_file.writestr('file1.xml', '<root>content1</root>')
        zip_file.writestr('file2.xml', '<root>content2</root>')
        zip_file.writestr('file3.txt', 'text file')
        zip_file.close()
        zip_file = zipfile.ZipFile('test.zip', 'r')
        xml_files = extract_xml_from_zip(zip_file)
        self.assertIsInstance(xml_files, list)
        self.assertEqual(len(xml_files), 2)
        self.assertIn('file1.xml', xml_files)
        self.assertIn('file2.xml', xml_files)
        zip_file.close()
        os.remove('test.zip')
        os.remove('file1.xml')
        os.remove('file2.xml')

# Create a sample XML file
        with open('test_xml.xml', 'w', encoding='utf-8') as f:
            f.write('''<?xml version="1.0" encoding="UTF-8"?><Pyld><Document xsi:schemaLocation="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02auth.036.001.02_ESMAUG_DLTINS_1.1.0.xsd" xmlns="urn:iso:std:iso:20022:tech:xsd:auth.036.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><FinInstrm> <TermntdRcrd><FinInstrmGnlAttrbts><Id>12345</Id><FullNm>Test Instrument</FullNm><ClssfctnTp>Test Classification Type</ClssfctnTp><NtnlCcy>Test National Currency</NtnlCcy><CmmdtyDerivInd>Test Commodity Derivative Indicator</CmmdtyDerivInd></FinInstrmGnlAttrbts><Issr>Test Issuer</Issr></TermntdRcrd></FinInstrm></Document></Pyld>''')

        # Call the function to parse the XML file
        extracted_data = parse_extracted_xml('test_xml.xml')

        # Define the expected extracted data
        expected_data = [{
            "FinInstrmGnlAttrbts.Id": "12345",
            "FinInstrmGnlAttrbts.FullNm": "Test Instrument",
            "FinInstrmGnlAttrbts.ClssfctnTp": "Test Classification Type",
            "FinInstrmGnlAttrbts.CmmdtyDerivInd": "Test Commodity Derivative Indicator",
            'FinInstrmGnlAttrbts.NtnlCcy': "Test National Currency",
            "Issr": "Test Issuer"
        }]

        # Assert that the extracted data matches the expected data
        self.assertEqual(extracted_data, expected_data)
        os.remove('test_xml.xml')

    def test_create_csv_and_write_data(self):
        self.csv_file = 'test.csv'
        self.data = [
            {
                "FinInstrmGnlAttrbts.Id": "1",
                "FinInstrmGnlAttrbts.FullNm": "Test1",
                "FinInstrmGnlAttrbts.ClssfctnTp": "Type1",
                "FinInstrmGnlAttrbts.CmmdtyDerivInd": "Yes",
                "FinInstrmGnlAttrbts.NtnlCcy": "USD",
                "Issr": "Issuer1"
            },
            {
                "FinInstrmGnlAttrbts.Id": "2",
                "FinInstrmGnlAttrbts.FullNm": "Test2",
                "FinInstrmGnlAttrbts.ClssfctnTp": "Type2",
                "FinInstrmGnlAttrbts.CmmdtyDerivInd": "No",
                "FinInstrmGnlAttrbts.NtnlCcy": "EUR",
                "Issr": "Issuer2"
            }
        ]

        create_csv_and_write_data(self.csv_file, self.data)
        self.assertTrue(os.path.exists(self.csv_file))
        if os.path.exists(self.csv_file):
            os.remove(self.csv_file)
