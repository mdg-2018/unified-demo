#!/bin/python3

import sys
import cluster
import archive

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
    cluster_mgr = cluster.cluster_manager(auth)
    oa_mgr = archive.archive_manager(auth,"demo-cluster")

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
    if action == "down":
        response = {
            "archive": oa_mgr.remove_archive(),
            "cluster": cluster_mgr.delete_cluster(),
        }
        
        print(response)

main()
