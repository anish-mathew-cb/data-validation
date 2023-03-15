# needed for any cluster connection
import json
import hashlib
from datetime import timedelta
import cb_connector as cb4_1
# from cbsdk4_1 import cb_connector as cb4_1
from cbsdk3_2 import cb_connector as cb3_2

import yaml


final_result = []

def load_config_from_yaml(filename):
    config = {}
    with open(f'../{filename}.yaml','r') as f: 
        config = yaml.safe_load(f)
    print('done!') 
    return config

def load_config_from_json(filename):
    config = {}
    with open(f'{filename}.json','r') as f: 
        config = json.load(f)
    print('done!') 
    return config

def get_colls_docs(bucket):
    doc_set_bucket = []
    doc_set_coll = {}

    if "documents" in bucket:
        doc_set_coll["bucket"] = bucket["name"]
        doc_set_coll["scope"]= ""
        doc_set_coll["collection"] = ""
        doc_set_coll["documents"] = bucket["documents"]
        doc_set_bucket.append(doc_set_coll)
        # print(json.dumps(doc_set_bucket, indent=2))
    else:
       for scope in bucket["scopes"]:
            for coll in scope["collections"]:
                doc_set_coll["bucket"] = bucket["name"]
                doc_set_coll["scope"]= scope["name"]
                doc_set_coll["collection"] = coll["name"]
                doc_set_coll["documents"] = coll["documents"]
                doc_set_bucket.append(doc_set_coll)
    return doc_set_bucket 

def compare(bucket):
    coll_documents = get_colls_docs(bucket)

    # Open the "default" bucket on both clusters
    for coll in coll_documents:
        result_parent = {}
        source.set_bucket(coll["bucket"])
        source.set_collection(coll["scope"], coll["collection"])

        target.set_bucket(coll["bucket"])
        target.set_collection(coll["scope"], coll["collection"])

        result_parent["bucket"] = coll["bucket"]
        result_parent["scope"] = coll["scope"]
        result_parent["collection"] = coll["collection"]
        
        compare_docs(result_parent, coll["documents"])
       

def compare_docs(result_parent, docs):
    for doc in docs:
        
        src_doc = source.get_doc(doc)
        tgt_doc = target.get_doc(doc)

        src_hash_json = json.dumps(src_doc, sort_keys=True)
        tgt_hash_json = json.dumps(tgt_doc, sort_keys=True)

        src_hash = hashlib.md5(src_hash_json.encode("utf-8")).hexdigest()
        tgt_hash = hashlib.md5(tgt_hash_json.encode("utf-8")).hexdigest()
        
        if src_hash != tgt_hash:
            result = {}

            result["bucket"] = result_parent["bucket"]
            result["scope"] = result_parent["scope"]
            result["collection"] = result_parent["collection"]
            result["doc_id"] = doc      
            result["src_doc"] = json.loads(src_hash_json)      
            result["tgt_doc"] = json.loads(tgt_hash_json)      

            print("src_hash_json", (src_hash_json))
            print("tgt_hash_json", (tgt_hash_json))

            print("result", json.dumps(result))
            final_result.append(result)


try: 
    # Load config from yaml
    config = load_config_from_yaml("./properties")

    #  nodes, bucket, scope, collection, username, password, secured
    source = cb4_1.CBConnector412(config["source"]["nodes"], config["source"]["username"], config["source"]["password"], config["source"]["secured"]) if config["source"]["version"] > 6.6 else cb3_2.CBConnector32(config["source"]["nodes"], config["source"]["username"], config["source"]["password"], config["source"]["secured"])

    target =  cb4_1.CBConnector412(config["target"]["nodes"], config["target"]["username"], config["target"]["password"], config["target"]["secured"]) if config["target"]["version"] > 6.6 else cb3_2.CBConnector32(config["target"]["nodes"], config["target"]["username"], config["target"]["password"], config["target"]["secured"])


    for bucket in config["source"]["buckets"]:
        print("="*50)
        print("BUCKET: ", bucket["name"])
        compare(bucket)

    with open('../final_result.txt', 'w') as file:
        json.dump(final_result, file, indent=2)

except Exception as e:
    print(e)