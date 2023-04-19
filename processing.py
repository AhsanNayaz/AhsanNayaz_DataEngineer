import requests
import io
import zipfile
import xml.etree.ElementTree as et
import csv
import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def process_xml_data(url, csv_file, bucket_name, key1, key2):
    """
    Processes XML data from a given URL, extracts relevant information, and writes it to a CSV file.

    Args:
        url (str): The URL from which the XML data will be downloaded.
        csv_file (str): The local file path of the CSV file where the extracted data will be written.
        bucket_name (str): The name of the S3 bucket where the CSV file will be uploaded.
        key1 (str): First key for AWS S3 access.
        key2 (str): Second key for AWS S3 access.

    Returns:
        None

    Raises:
        requests.exceptions.RequestException: If there is an error while making HTTP requests to download XML or ZIP files.
        xml.etree.ElementTree.ParseError: If there is an error while parsing the XML content.
        zipfile.BadZipFile: If the downloaded ZIP file is corrupt or invalid.
        FileNotFoundError: If the downloaded XML or ZIP files are not found at the specified paths.
        Exception: For any other unexpected errors.

    Example:
        >>> process_xml_data('https://example.com/xml_data.xml', 'output.csv', 'key1', 'key2')

    Note:
        This function assumes that the XML data at the given URL follows a specific structure, and that the ZIP file
        extracted from that URL contains XML files that can be parsed using ElementTree module.
    """
    try:
        # Download XML content
        xml_content = download_xml(url)

        # Parse XML content
        xml_root = parse_xml(xml_content)

        # Find download link in XML content
        zip_url = find_download_link(xml_root)

        # Download ZIP file
        zip_file = download_zip(zip_url)

        # Extract XML files from ZIP
        xml_files = extract_xml_from_zip(zip_file)

        # Extract and parse data from XML files
        extracted_data = []
        for xml_file in xml_files:
            extracted_data.extend(parse_extracted_xml(xml_file))

        # Create CSV file and write extracted data
        create_csv_and_write_data(csv_file, extracted_data)

        # Upload CSV file to S3
        upload_csv_to_s3(bucket_name, csv_file, csv_file, key1, key2)

    except requests.exceptions.RequestException as e:
        # Handle HTTP request errors
        logger.info(f"Error while making HTTP request: {e}")
        raise

    except et.ParseError as e:
        # Handle XML parsing errors
        logger.info(f"Error while parsing XML content: {e}")
        raise

    except zipfile.BadZipFile as e:
        # Handle ZIP file errors
        logger.info(f"Error while extracting ZIP file: {e}")
        raise

    except FileNotFoundError as e:
        # Handle file not found errors
        logger.info(f"Error while accessing file: {e}")
        raise

    except Exception as e:
        # Handle unexpected errors
        logger.info(f"Unexpected error: {e}")
        raise


def download_xml(url):
    """
    Downloads XML content from a given URL and returns the response content.

    Args:
        url (str): The URL from which the XML content will be downloaded.

    Returns:
        bytes: The response content as bytes.

    Raises:
        requests.exceptions.RequestException: If there is an error while making the HTTP request.
        Exception: For any other unexpected errors.

    Example:
        >>> xml_content = download_xml('https://example.com/xml_data.xml')

    Note:
        This function assumes that the provided URL points to a valid XML resource that can be accessed using HTTP/HTTPS.
    """
    try:
        # Make HTTP request to download XML
        response = requests.get(url)

        # Raise exception if HTTP request returns an error status code
        response.raise_for_status()
        logger.info(f"XML file downloaded from: {url}")
        # Return response content as bytes
        return response.content

    except requests.exceptions.RequestException as e:
        # Handle HTTP request errors
        logger.info(f"Error while making HTTP request: {e}")
        raise

    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error: {e}")
        raise


def parse_xml(xml_content):
    """
    Parses the given XML content and returns the root element.

    Args:
        xml_content (bytes): The XML content to be parsed as bytes.

    Returns:
        Element: The root element of the parsed XML.

    Raises:
        Exception: If there is an error while parsing the XML.

    Example:
        >>> xml_root = parse_xml(xml_content)

    Note:
        This function assumes that the provided XML content is in valid XML format.

    """
    try:
        # Parse XML content and return root element
        root = et.fromstring(xml_content)
        logger.info(f"XML file parsed")
        return root

    except Exception as e:
        # Handle XML parsing errors
        logger.info(f"Failed to parse XML: {e}")
        raise


def find_download_link(xml_root):
    """
    Searches the XML root element for the download link value and returns it.

    Args:
        xml_root (Element): The root element of the XML to search.

    Returns:
        str: The value of the download link.

    Raises:
        ValueError: If no download link is found in the XML.

    Example:
        >>> download_link = find_download_link(xml_root)

    Note:
        This function assumes that the provided XML root element is valid and contains the expected structure.

    """
    for elem in xml_root.iter("str"):
        if elem.tag == "str" and elem.attrib.get("name") == "download_link":
            logger.info(f"Download link found: {elem.text}")
            return elem.text

    # Handle download link not found error
    logger.error("No download link found in XML")
    raise ValueError("No download link found in XML")


def download_zip(zip_url):
    """
    Downloads a ZIP file from the provided URL and returns a ZipFile object.

    Args:
        zip_url (str): The URL of the ZIP file to download.

    Returns:
        zipfile.ZipFile: A ZipFile object representing the downloaded ZIP file.

    Raises:
        Exception: If failed to download the ZIP file.

    Example:
        >>> zip_file = download_zip(zip_url)

    Note:
        This function uses the 'requests' library to download the ZIP file in chunks to avoid memory issues.
        The downloaded content is then wrapped in a ZipFile object and returned.

    """
    try:
        response = requests.get(zip_url, stream=True)
        response.raise_for_status()

        # Read the content in chunks to avoid memory issues
        content = io.BytesIO()
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                content.write(chunk)
        content.seek(0)

        # Open the zip file
        zip_file = zipfile.ZipFile(content, 'r')
        logger.info(f"ZIP file downloaded from: {zip_url}")
        return zip_file

    except Exception as e:
        logger.error(f"Failed to download ZIP file: {str(e)}")
        raise


def extract_xml_from_zip(zip_file):
    """
    Extracts XML files from a ZipFile object and returns a list of extracted file names.

    Args:
        zip_file (zipfile.ZipFile): The ZipFile object representing the ZIP file.

    Returns:
        list: A list of extracted XML file names.

    Raises:
        Exception: If failed to extract XML files from the ZIP file.

    Example:
        >>> xml_files = extract_xml_from_zip(zip_file)

    Note:
        This function iterates over the files in the ZipFile object and checks if the file name ends with '.xml'.
        If so, it extracts the file and appends the file name to the list of extracted XML files.
        The extracted file names are returned as a list.

    """
    try:
        xml_files = []
        for file_name in zip_file.namelist():
            if file_name.endswith('.xml'):
                zip_file.extract(file_name)
                xml_files.append(file_name)
        logger.info(f"XML extracted from ZIP")
        return xml_files

    except Exception as e:
        logger.error(f'Failed to extract XML files from ZIP: {str(e)}')
        raise


def parse_extracted_xml(xml_file):
    """
    Parses the extracted XML file and extracts the required data.

    Args:
        xml_file (str): Path to the extracted XML file.

    Returns:
        List[Dict[str, str]]: List of dictionaries containing the extracted data.

    Raises:
        Exception: If failed to parse or extract data from the XML file.

    Example:
        >>> extracted_data = parse_extracted_xml(xml_file)

    Note:
        This function reads the content of the XML file using the provided file path and UTF-8 encoding.
        It then parses the XML content and extracts the required data using ElementTree (et) module.
        The extracted data is returned as a list of dictionaries, where each dictionary contains the extracted data
        with key-value pairs.
        This function assumes a specific structure of the XML file with defined namespaces (ns) and XPath expressions
        to extract the required data from the XML file.

    """
    try:
        with open(xml_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()

        # Define namespace dictionary
        ns = {
            'ns': 'urn:iso:std:iso:20022:tech:xsd:head.003.001.01',
            'ns2': 'urn:iso:std:iso:20022:tech:xsd:auth.036.001.02',
            'ns3': 'urn:iso:std:iso:20022:tech:xsd:auth.037.001.02',
            'ns4': 'urn:iso:std:iso:20022:tech:xsd:coll.018.001.01',
            'ns5': 'urn:iso:std:iso:20022:tech:xsd:secval.010.001.02'
        }

        root = et.fromstring(xml_content)
        extracted_data = []

        for instrm in root.findall(".//ns2:FinInstrm", ns):
            data = {
                "FinInstrmGnlAttrbts.Id": instrm.find(".//ns2:Id", ns).text,
                "FinInstrmGnlAttrbts.FullNm": instrm.find(".//ns2:FullNm", ns).text,
                "FinInstrmGnlAttrbts.ClssfctnTp": instrm.find(".//ns2:ClssfctnTp", ns).text,
                "FinInstrmGnlAttrbts.CmmdtyDerivInd": instrm.find(".//ns2:CmmdtyDerivInd", ns).text,
                'FinInstrmGnlAttrbts.NtnlCcy': instrm.find(".//ns2:NtnlCcy", ns).text,
                "Issr": instrm.find(".//ns2:Issr", ns).text
            }

            extracted_data.append(data)
        logger.info(f"XML parsed and data extracted using namespace")
        return extracted_data

    except Exception as e:
        logger.error(f"Failed to parse or extract data from XML: {str(e)}")
        raise


def create_csv_and_write_data(csv_file, data):
    """
    Creates a CSV file and writes the extracted data to it.

    Args:
        csv_file (str): Path to the CSV file to be created.
        data (List[Dict[str, str]]): List of dictionaries containing the data to be written to the CSV file.

    Raises:
        ValueError: If the provided data is not a list of dictionaries.
        IOError: If there is an error while creating or writing to the CSV file.
    """
    if not isinstance(data, list):
        raise ValueError("Data should be a list of dictionaries.")

    csv_headers = [
        "FinInstrmGnlAttrbts.Id",
        "FinInstrmGnlAttrbts.FullNm",
        "FinInstrmGnlAttrbts.ClssfctnTp",
        "FinInstrmGnlAttrbts.CmmdtyDerivInd",
        "FinInstrmGnlAttrbts.NtnlCcy",
        "Issr"
    ]

    try:
        with open(csv_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=csv_headers)
            writer.writeheader()
            writer.writerows(data)
        logging.info(f"Data written to CSV file: {csv_file}")
    except IOError as e:
        logging.error(f"Error while creating or writing to CSV file: {csv_file}. {e}")
        raise IOError(f"Error while creating or writing to CSV file: {csv_file}. {e}")


def upload_csv_to_s3(bucket_name, csv_file, s3_key, key1, key2):
    """
    Uploads a CSV file to an AWS S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        csv_file (str): The path of the CSV file to upload.
        s3_key (str): The S3 key to use for the uploaded file.
        key1 (str): The access key id for the AWS S3 account.
        key2 (str): The secret access key for the AWS S3 account.

    Returns:
        None

    Raises:
        Exception: If there is an error while uploading the CSV file to S3.
    """
    s3_client = boto3.client('s3', aws_access_key_id=key1,
                             aws_secret_access_key=key2)
    try:
        # Upload CSV file to S3
        s3_client.upload_file(csv_file, bucket_name, s3_key)
        logging.info(f"Successfully uploaded CSV file to S3 bucket: {bucket_name}, S3 key: {s3_key}")
    except Exception as e:
        logging.error(f"Failed to upload CSV file to S3: {str(e)}")
        raise Exception(f"Failed to upload CSV file to S3: {str(e)}")
