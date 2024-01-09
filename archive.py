import requests
from time import sleep
from requests.auth import HTTPDigestAuth
from datetime import datetime

class archive_manager:
    def __init__(self,auth,cluster_name):
        self.auth = auth
        self.cluster_name = cluster_name
        self.id = None

    def create_archive(self):
        url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/clusters/{cluster_name}/onlineArchives".format(groupId = self.auth["group_id"],cluster_name = self.cluster_name)
        exp_days = datetime.now() - datetime(2016,1,1)
        oa_data = {
            "collName": "transactions",
            "collectionType": "STANDARD",
            "criteria": {
                "type": "DATE",
                "dateField": "bucket_end_date",
                "expireAfterDays": exp_days.days
            },
            "dataProcessRegion": {
                "cloudProvider": "AWS",
                "region": "US_EAST_1"
            },
            "dbName": "sample_analytics",
            "partitionFields": [
                {
                    "fieldName":"bucket_end_date",
                    "order": 0
                }
            ],
            "schedule": {
                "type": "DEFAULT"
            }
            }
        response = requests.post(url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),json=oa_data,headers={"Accept":"application/vnd.atlas.2023-02-01+json"})

        self.id = response.json()["_id"]

        return response.json()
    
    def get_archive_status(self,id=None):
        if not id:
            id = self.id
        url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/clusters/{cluster_name}/onlineArchives/{id}".format(groupId = self.auth["group_id"],cluster_name = self.cluster_name, id=id)
        response = requests.get(url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),headers={"Accept":"application/vnd.atlas.2023-02-01+json"})
        return response.json()

    
    def remove_archive(self):
        url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/clusters/{cluster_name}/onlineArchives".format(groupId = self.auth["group_id"],cluster_name = self.cluster_name)
        response = requests.get(url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),headers={"Accept":"application/vnd.atlas.2023-02-01+json"})
        results = []
        for archive in response.json()["results"]:
            delete_url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/clusters/{cluster_name}/onlineArchives/{id}".format(groupId = self.auth["group_id"],cluster_name = self.cluster_name,id=archive["_id"])
            delete_response = requests.delete(delete_url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),headers={"Accept":"application/vnd.atlas.2023-02-01+json"})
            results.append(delete_response.json())
        return results