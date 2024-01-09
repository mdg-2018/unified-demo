from pymongo import MongoClient
from cluster import cluster_manager
from time import sleep
from datetime import datetime

class workload_generator:
    def __init__(self,auth,cluster_name):
        # get cluster uri
        cm = cluster_manager(auth,cluster_name)
        self.uri = cm.get_cluster()["connectionStrings"]["standard"]
        self.auth = auth


    def print_uri(self):
        return print(self.uri)
    
    def setup_profiler(self):
        client = MongoClient(self.uri, username=self.auth["mongo_user"], password=self.auth["mongo_pass"])
        
        #set profiling level to 2
        client["sample_mflix"].set_profiling_level(2,slow_ms=0)
        
        ### Setup and run inefficient queries ###
        collection = client["sample_mflix"]["movies"]

        # Create giant collection for slow queries
        collection.aggregate([
            {"$project":{"_id":0}},
            {"$addFields":{"split_plot":{"$split":["$plot"," "]}}},
            {"$unwind":"$split_plot"},
            {"$out":"big_movies"}
        ])

        # Get array of cast members
        cast_members = []
        cast_member_cursor = collection.aggregate([
            {
                "$unwind":{
                    "path":"$cast"
                }
            },
            {
                "$group":{
                    "_id":"$cast"
                }
            }
        ])
        
        for actor in cast_member_cursor:
            cast_members.append(actor["_id"])

        print("Found " + str(len(cast_members)) + " actors.")

        # Query the new giant collection
        counter = 0
        collection = client["sample_mflix"]["big_movies"]
        for i in range(0,100):
            result = collection.find({"cast":cast_members[i]}).sort("year")
            for r in result:
                counter += 1
            print("Completed {num_queries} out of 100 queries.".format(num_queries=i))
            