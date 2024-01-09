import requests
from time import sleep
from requests.auth import HTTPDigestAuth

class cluster_manager:
  def __init__(self,auth):
    self.auth = auth
    self.sample_dataset_load_id = None
    self.defaultClusterSettings = {
      "clusterType": "REPLICASET",
      "links": [],
      "name": "demo-cluster",
      "replicationSpecs": [
        {
          "numShards": 1,

          "regionConfigs": [
            {
              "electableSpecs": {
                "instanceSize": "M10",
                "nodeCount": 3
              },
              "providerName": "AWS",
              "regionName": "US_EAST_1",
              "priority":7
            },
          ]
        }
      ]
    }
  
  def create_cluster(self,wait=False):
    url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/clusters".format(groupId = self.auth["group_id"])
    response = requests.post(url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),json=self.defaultClusterSettings,headers={"Accept":"application/vnd.atlas.2023-02-01+json"})
    if wait:
      print("Waiting for cluster")
    else:
      return response.json()
  
  def delete_cluster(self):
    url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/clusters/{name}".format(groupId = self.auth["group_id"],name=self.defaultClusterSettings["name"])
    response = requests.delete(url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),json=self.defaultClusterSettings,headers={"Accept":"application/vnd.atlas.2023-02-01+json"})
    return response.json()
  
  def load_sample_data(self):
    url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/sampleDatasetLoad/{name}".format(groupId = self.auth["group_id"],name=self.defaultClusterSettings["name"])
    response = requests.post(url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),headers={"Accept":"application/vnd.atlas.2023-02-01+json"})
    self.sample_dataset_load_id = response.json()["_id"]
    return response.json()

  def await_sample_data(self):
    id="659c1ca2b879202609975303"
    url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/sampleDatasetLoad/{id}".format(groupId = self.auth["group_id"],id=self.sample_dataset_load_id)
    ready = False
    seconds = 0
    while not ready:
      response = requests.get(url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),headers={"Accept":"application/vnd.atlas.2023-02-01+json"})
      if response.json()["state"] != "WORKING":
        print(response.json()["state"])
        return True
      else:
        print("Awaiting sample data, it's been " + str(seconds) + " seconds so far.")
        sleep(5)
        seconds += 5
  
  def await_cluster(self):
    url = "https://cloud.mongodb.com/api/atlas/v2/groups/{groupId}/clusters/{name}".format(groupId = self.auth["group_id"],name=self.defaultClusterSettings["name"])

    ready = False
    seconds = 0
    while not ready:
      response = requests.get(url,auth=HTTPDigestAuth(self.auth["public_key"],self.auth["private_key"]),headers={"Accept":"application/vnd.atlas.2023-02-01+json"})
      cluster_state = response.json()["stateName"]
      if cluster_state == "IDLE":
        return True
      else:
        print("Awaiting cluster, it's been " + str(seconds) + " seconds so far.")
        sleep(5)
        seconds += 5