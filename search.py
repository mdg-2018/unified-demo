import requests
from requests.auth import HTTPDigestAuth

class search_manager:
    def __init__(self,auth,cluster_name):
        self.auth = auth
        self.cluster_name = cluster_name

    def create_search_index(self):
        url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/clusters/{clusterName}/fts/indexes".format(groupId=self.auth.group_id,clusterName=self.cluster_name)

        idx_data = {
            # Todo: set this up for airbnb data
            {
                "collectionName": "string",
                "database": "string",
                "name": "string",
                "type": "search",
                "analyzer": "lucene.standard",
                "mappings": {
                    "dynamic": false,
                    "fields": {
                    "property1": {},
                    "property2": {}
                    }
                },
                "searchAnalyzer": "lucene.standard",
                }
        }

        search_idx = requests.post(url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),json=idx_data,headers={"Accept":"application/vnd.atlas.2023-02-01+json"})
        return search_idx