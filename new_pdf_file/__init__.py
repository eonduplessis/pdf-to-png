import logging
from pdf2image import convert_from_bytes

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

import io

import os

import azure.functions as func


def pdf_to_png(company_name, document_name, pdf_file_blob):
    pages = convert_from_bytes(pdf_file_blob.read(), fmt='png')

    connect_str = 'DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=unique54storagename;AccountKey=FffG8tJM8JJ2qP0iBgXETCSrMC6+6XQ25EgtedQqrabwx+fHjy7pILIbEFBtOnbrXX9fJddf5OsECtXkdprE/A=='

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    page_number = 0 

    container_name = 'companies-doc-images'

    local_file_list = company_name + document_name + '.txt'
    with open(local_file_list, 'w') as f:
        #write file names to complete.txt in sequence
        for page in pages:
            # Save pages as images in the pdf
            target_image_name = company_name + '/' + document_name + '/page'+ str(page_number) + '.png'
            local_image_name = company_name + document_name + str(page_number) + '.png'

            page.save(local_image_name, 'PNG')

            blob_client = blob_service_client.get_blob_client(container=container_name,  blob=target_image_name)

            with open(local_image_name, "rb") as data:
                blob_client.upload_blob(data)

            f.write(target_image_name)
            f.write('\n')

            os.remove(local_image_name)

            page_number = page_number + 1

    target_file_name = company_name + '/' + document_name + '/complete.txt'
    blob_client = blob_service_client.get_blob_client(container=container_name,  blob=target_file_name)

    with open(local_file_list, "rb") as data:
        blob_client.upload_blob(data)

    os.remove(local_file_list)


def main(inputblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {inputblob.name}\n"
                 f"Blob Size: {inputblob.length} bytes")

    #logging.info(f'Python Queue trigger function processed {len(inputblob)} bytes')
    #outputblob.set(inputblob)
    company = inputblob.name[15:-4]
    filename = 'test'

    pdf_to_png(company, 'testdocname', inputblob)

    
    """
    logging.info(f"Converting blob bytestream to PNG")
    image_list = pdf_to_png(inputblob.name[15:-4], inputblob)
    logging.info(f"Converting blob bytestream to PNG complete")
    """
