from processing import process_xml_data

key1 = "ENTER_ACCESS_KEY"
key2 = "ENTER_SECRET_KEY"
bucket_name = 'ENTER_BUCKET_NAME'
url = "https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01" \
      "-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"  # replace with your actual XML
csv_file = "output.csv"  # replace with desired output CSV file path

process_xml_data(url, csv_file, bucket_name, key1, key2)
