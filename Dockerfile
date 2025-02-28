# To enable ssh & remote debugging on app service change the base image to the one below
# FROM mcr.microsoft.com/azure-functions/python:3.0-python3.9-appservice
FROM mcr.microsoft.com/azure-functions/python:3.0-python3.9

ENV AzureWebJobsScriptRoot=/home/site/wwwroot \
    AzureFunctionsJobHost__Logging__Console__IsEnabled=true

RUN apt-get update && apt-get install -y --no-install-recommends build-essential libpq-dev poppler-utils
RUN pip3 install --upgrade pip

COPY requirements.txt /
RUN pip install -r /requirements.txt

COPY . /home/site/wwwroot