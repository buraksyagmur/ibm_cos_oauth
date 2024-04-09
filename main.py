import ibm_boto3
from ibm_botocore.client import Config, ClientError
import json


with open('./ibm_service_credentials.json') as file:
    data = json.load(file)
bucket_name = data['iam_apikey_name'].split("-", 2)[2]


cos = ibm_boto3.client("s3",
                       ibm_api_key_id=data["apikey"],
                       ibm_service_instance_id=data["resource_instance_id"],
                       config=Config(signature_version="oauth"),
                       endpoint_url='https://s3.eu-gb.cloud-object-storage.appdomain.cloud'
                       )

try:
    response = cos.list_buckets()
    for bucket in response['Buckets']:
        print("Bucket Name: {0}".format(bucket['Name']))
except ClientError as be:
    print("CLIENT ERROR: {0}\n".format(be))
except Exception as e:
    print("Unable to retrieve list buckets: {0}".format(e))

try:
    id = 'test'
    file_name = f'{id}/nameofthefile.csv'
    local_file_path = 'filepathofthefile'

    with open(local_file_path, "rb") as file_data:
        cos.put_object(Bucket=bucket_name, Key=file_name, Body=file_data)
except ClientError as be:
    print("CLIENT ERROR: {0}\n".format(be))
except Exception as e:
    print("Unable to create text file: {0}".format(e))

try:
    response = cos.list_objects_v2(Bucket=bucket_name)
    if 'Contents' in response:
        print("Objects in the: {0}".format(bucket_name))
        for obj in response['Contents']:
            print("Object Key: {0}".format(obj['Key']))
    else:
        print("The bucket is empty.")
except ClientError as be:
    print("CLIENT ERROR: {0}\n".format(be))
except Exception as e:
    print("Unable to list objects in the bucket: {0}".format(e))
