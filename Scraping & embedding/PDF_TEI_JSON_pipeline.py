from grobid_client.grobid_client import GrobidClient
import xmltodict
import json
import os

def process_pdfs(pdf_folder_path, tei_folder_path, json_folder_path):
    # Initialize the GROBID client
    client = GrobidClient()

    # Ensure output directories exist
    os.makedirs(tei_folder_path, exist_ok=True)
    os.makedirs(json_folder_path, exist_ok=True)

    # Process each PDF in the specified folder
    for pdf_filename in os.listdir(pdf_folder_path):
        if pdf_filename.endswith('.pdf'):
            pdf_path = os.path.join(pdf_folder_path, pdf_filename)
            tei_filename = pdf_filename.replace('.pdf', '.tei.xml')
            json_filename = pdf_filename.replace('.pdf', '.json')

            tei_file_path = os.path.join(tei_folder_path, tei_filename)
            json_file_path = os.path.join(json_folder_path, json_filename)

            # Process PDF to get TEI XML
            client.process("processFulltextDocument", pdf_path, output=tei_folder_path)

            # Convert TEI XML to JSON
            tei_to_json(tei_file_path, json_file_path)

def tei_to_json(tei_file_path, json_file_path):
    # Read the TEI XML file
    with open(tei_file_path, 'r', encoding='utf-8') as tei_file:
        tei_content = tei_file.read()

    # Convert XML to a dictionary
    tei_dict = xmltodict.parse(tei_content)

    # Convert dictionary to JSON
    json_content = json.dumps(tei_dict, indent=4)

    # Write JSON to a file
    with open(json_file_path, 'w', encoding='utf-8') as json_file:
        json_file.write(json_content)

# Example usage
pdf_folder_path = 'path/to/your/pdf/folder'
tei_folder_path = 'path/to/your/tei/folder'
json_folder_path = 'path/to/your/json/folder'

process_pdfs(pdf_folder_path, tei_folder_path, json_folder_path)
