# AhsanNayaz_DataEngineer

This project is designed to download an XML file from a given link, parse through the XML data, and convert it into a CSV file with specific headers. The generated CSV file is then stored in an AWS S3 bucket. The project has alsôbeen implemented as a AWS Lambda function.

## Requirements

- Python 3.x
- Dependencies:
  - [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) (AWS SDK for Python)
  - [Requests](https://docs.python-requests.org/en/latest/) (for making HTTP requests)
  - [ZipFile](https://docs.python.org/3/library/zipfile.html) (for extracting zip files)
  - [xml.etree.ElementTree](https://docs.python.org/3/library/xml.etree.elementtree.html) (for parsing XML data)
  - [csv](https://docs.python.org/3/library/csv.html) (for working with CSV files)

## Usage

1. Clone this repository to your local machine.
2. Install the required dependencies using `pip` or any other preferred package manager.
3. Update the `main.py` file with your AWS S3 credentials (access key and secret access key) and specify the desired AWS S3 bucket name.
4. Run the `main.py` script using Python to start the process of downloading the XML file, parsing the data, converting it into a CSV file, and storing it in the AWS S3 bucket.
5. The generated CSV file will be stored in the specified AWS S3 bucket upon successful execution of the `main.py` script.
6. You can also run the unit tests in `unittest.py` to verify the functionality of the code.

## Configuration

The following configuration can be updated in the `main.py` file:

- `key1`: AWS access key
- `key2`: AWS secret access key
- `bucket_name`: AWS S3 bucket name to store the generated CSV file
- `url`: URL of the XML file to download
- `csv_file`: File path of the output CSV file

## Live

This project has been deployed as a AWS Lambda function and can be found [here](https://o5fcvq4skj5eprjmp3q4b3rznq0pljds.lambda-url.ap-southeast-1.on.aws/)

## Contributing

Contributions to this project are welcome! If you find any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request.
