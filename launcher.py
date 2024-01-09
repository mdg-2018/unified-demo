#!/bin/python3

import sys
import cluster
import archive
import search
import workload
from datetime import datetime

def main():

    auth = {
        "group_id":"",
        "public_key":"",
        "private_key":"",
        "mongo_user":"launcher",
        "mongo_pass":"Kd3R8w7FTG9bkY"
    }

    auth["group_id"] = sys.argv[1]
    auth["public_key"] = sys.argv[2]
    auth["private_key"] = sys.argv[3]
    try:
        action=sys.argv[4]
    except:
        print("You forgot to specify an action (up or down)")
        exit()

    print(auth)

    # Create manager objects
    # Global cluster name
    cluster_name = "demo-cluster-" + str(datetime.now().hour) + str(datetime.now().minute)

    cluster_mgr = cluster.cluster_manager(auth,cluster_name)
    oa_mgr = archive.archive_manager(auth,cluster_name)
    search_mgr = search.search_manager(auth,cluster_name)
    

    if action == "up":
        #Launch all the stuff

        #Create cluster & load data
        cluster_response = cluster_mgr.create_cluster()
        print(cluster_response)
        cluster_mgr.await_cluster()
        sample_data = cluster_mgr.load_sample_data()
        print(sample_data)
        print(cluster_mgr.await_sample_data())

        #Setup Archive
        print(oa_mgr.create_archive())

        #Create search index
        print(search_mgr.create_search_index())

        #Populate profiler and performance advisor
        #Workload manager can't be instantiated until the cluster is ready
        workload_mgr = workload.workload_generator(auth,cluster_name)
        print(workload_mgr.setup_profiler())


    if action == "down":
        response = {
            "cluster": cluster_mgr.delete_clusters(),
        }
        
        print(response)

main()
