"""Parameterize the Pipeline name for which the code needs to pull the Datasets and Pipeline Definition
   Parameterize the Keyword that needs to be searched"""

import requests
import json

azure_client_id = '' # to be filled in
azure_secret = '' # to be filled in
azure_subscription_id = '' # to be filled in
azure_tenant = '' # to be filled in
resource_group_name = '' # to be filled in


def search_datasets(pipeline_def, keyword):
    for activity in pipeline_def['properties']['activities']:
        if activity['name'].lower().find(keyword.lower()) >= 0:
            return 1



def process_datasets(pipeline_def, headers_info):
    print("Captured Single Pipeline and following are the Datasets:")
    for activity in pipeline_def['properties']['activities']:
        if 'inputs' in activity.keys():
            for inputs in activity['inputs']:
                if inputs['type'] == "DatasetReference":
                    print(inputs['referenceName'])
                    url = "" # REST API - URL
                    dataset = requests.get(url, headers=headers_info).json()
                    dataset.pop('id', None)
                    dataset.pop('etag', None)
                    dataset.pop('name', None)
                    dataset.pop('type', None)
                    if dataset["properties"]["linkedServiceName"]["referenceName"] == "": # Use this line of code if Linked Service needs to be updated
                        dataset["properties"]["linkedServiceName"]["referenceName"] = ""
                    url = "" # REST API - URL
                    body = json.dumps(dataset)
                    new_dataset = requests.put(url, headers=headers_info, data=body).json()
        if 'outputs' in activity.keys():
            for outputs in activity['outputs']:
                if outputs['type'] == "DatasetReference":
                    print(outputs['referenceName'])
                    url = "" # REST API - URL
                    dataset = requests.get(url, headers=headers_info).json()
                    dataset.pop('id', None)
                    dataset.pop('etag', None)
                    dataset.pop('name', None)
                    dataset.pop('type', None)
                    url = "" // REST API - URL
                    body = json.dumps(dataset)
                    new_dataset = requests.put(url, headers=headers_info, data=body).json()


url = "https://login.microsoftonline.com/" + azure_tenant + "/oauth2/token"
data = "resource=https://management.core.windows.net/&client_id=" + azure_client_id + \
       "&grant_type=client_credentials&client_secret=" + azure_secret
headers = {'Content-Type': 'application/x-www-form-urlencoded'}
response = requests.post(url, data=data, headers=headers)


headers = {'Authorization': 'Bearer ' + response.json()['access_token'], 'Content-Type': 'application/json',
           'Accept': 'application/json'}

url = "" // REST API - URL
pipeline_dict = requests.get(url, headers=headers).json()
keyword = '' // Table Name to be sear
while 'nextLink' in pipeline_dict.keys():
    pipeline_list = pipeline_dict['value']
    for pipeline in pipeline_list:
        if pipeline['name'] == "": # Pipeline name to be deployed
        #if pipeline['name'].find('exdb') >= 0:
            pipeline.pop('id', None)
            pipeline.pop('etag', None)
            pipeline.pop('type', None)
            print('Pipeline Name: ', pipeline['name'])
            process_datasets(pipeline, headers)  # Get Datasets for pipeline to be deployed
            # Code for Searching metadata of Pipelines
            #search_result = search_datasets(pipeline, keyword)
            #if search_result == 1:
                #print('Activity found in Pipeline: ', pipeline['name'])
            # End of Code for searching metadata of Pipelines
            # Code to Deploy the pipeline to a different Data Factory (QA / PROD)
            url = "" # PROD REST - API URL
            pipeline.pop('name', None)
            body = json.dumps(pipeline)
            print(body)
            new_pipeline = requests.put(url, headers=headers, data=body).json() # Deploy pipeline to PROD
            print(new_pipeline)
            exit(0)

    pipeline_dict = requests.get(pipeline_dict['nextLink'], headers=headers).json()
